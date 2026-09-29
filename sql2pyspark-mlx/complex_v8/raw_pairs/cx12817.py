from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

with_prev = cleaned.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_quality_running))

sessions = (
    sessionized
    .groupBy("quality", "session_id")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

screened = (
    sessions
    .crossJoin(sessions.agg(F.min("duration_min").alias("overall_min_duration_min")))
    .filter(F.col("duration_min") > F.col("overall_min_duration_min"))
    .drop("overall_min_duration_min")
)

derived = (
    screened
    .filter(F.col("duration_min") < 192.0)
    .withColumn("duration_min_filled", F.coalesce(F.col("duration_min"), F.lit(0.0)))
)

result = (
    derived
    .cube("quality", "events")
    .agg(
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.countDistinct("quality").alias("distinct_qualitys"),
    )
    .orderBy(F.col("quality").asc_nulls_last(), F.col("events").asc_nulls_last())
)
