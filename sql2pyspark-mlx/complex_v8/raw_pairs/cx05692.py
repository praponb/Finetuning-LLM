from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_meter_readings
    .filter(F.col("kwh").between(12.2, 117.2))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
    )
)

with_prev = cleaned.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_site_id_running))

combined_details_site = (
    sessionized.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.tariff").isNotNull())
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality_ref",
        "s.prev_read_at",
        "s.starts_session",
        "s.session_no",
        "si.site_name",
        F.col("si.tariff").alias("site_tariff"),
    )
)

result = (
    combined_details_site
    .filter(F.col("voltage").between(221.4, 242.3))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality_ref",
        "prev_read_at",
        "starts_session",
        "session_no",
        "site_tariff",
    )
    .orderBy(F.col("quality_ref").desc(), F.col("read_at").desc())
)
