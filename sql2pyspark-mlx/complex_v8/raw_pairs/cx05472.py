from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_readings = (
    df_meter_readings
    .filter(F.col("kwh") > 51.5)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("voltage") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("voltage_per_kwh"),
    )
)

with_prev = valid_readings.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_quality_running))

per_session = (
    sessionized
    .groupBy("quality", "session_no")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

enriched = (
    per_session
    .select(
        "*",
        F.concat(F.col("quality"), F.lit("#"), F.col("session_no").cast("string")).alias("quality_ref"),
        F.abs(F.col("session_no") - 4).alias("session_no_distance"),
    )
)

result = (
    enriched
    .filter(F.col("session_no") <= 5)
    .select(
        "quality",
        "session_no",
        "session_start",
        "session_end",
        "duration_min",
        "quality_ref",
        "session_no_distance",
        F.coalesce(F.col("session_no_distance"), F.lit(0)).alias("session_no_distance_filled"),
    )
    .orderBy(F.col("quality_ref").desc_nulls_first(), "session_no_distance")
)
