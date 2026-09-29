from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_readings = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
    )
)

with_prev = prepared_readings.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_quality_running))

enriched = (
    sessionized.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.upper(F.trim(F.col("si.site_name"))).like("R%"))
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.quality_prefix",
        "s.prev_read_at",
        "s.is_new_session",
        "s.session_id",
        "si.site_name",
    )
)

result = (
    enriched
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "is_new_session",
        "site_name",
        F.greatest(F.col("session_id"), F.col("is_new_session")).alias("greatest_session_id_is_new_session"),
    )
    .orderBy(F.col("site_id").desc(), F.col("read_at").asc())
)
