from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

with_prev = recent.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_site_id_running))

line_level_readings = (
    sessionized.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality") != "OK"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.is_new_session",
        "s.session_no",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.quality").alias("reading_quality"),
    )
)

totals = (
    line_level_readings
    .groupBy("is_new_session")
    .agg(
        F.sum("reading_voltage").alias("total_reading_voltage"),
        F.round(F.avg("is_new_session"), 2).alias("avg_is_new_session"),
    )
)

result = (
    totals
    .filter(F.col("avg_is_new_session") >= 0.1)
    .select(
        "is_new_session",
        "total_reading_voltage",
        (F.col("total_reading_voltage") + F.col("is_new_session")).alias("total_reading_voltage_plus_is_new_session"),
        F.round(F.col("avg_is_new_session") / F.nullif(F.col("is_new_session"), F.lit(0)), 3).alias("avg_is_new_session_per_is_new_session"),
    )
    .orderBy(
        F.col("total_reading_voltage_plus_is_new_session").desc(),
        "avg_is_new_session_per_is_new_session",
    )
)
