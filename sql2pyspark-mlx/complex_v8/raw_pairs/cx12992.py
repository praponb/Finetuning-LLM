from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_readings = df_meter_readings.filter(~F.col("quality").isin("EST", "OK"))

with_prev = scoped_readings.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_quality_running))

session_summary = (
    sessionized
    .groupBy("quality", "burst_id")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("voltage").alias("session_voltage"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

grouped = (
    session_summary
    .groupBy("quality")
    .agg(
        F.max("session_start").alias("last_session_start"),
        F.count("*").alias("row_count"),
        F.countDistinct("quality").alias("distinct_qualitys"),
        F.min("duration_min").alias("min_duration_min"),
        F.count(F.when(F.col("quality") == "EST", 1)).alias("n_quality_est"),
        F.max("n_events").alias("max_n_events_check"),
    )
    .filter(F.col("max_n_events_check") > 2)
    .drop("max_n_events_check")
)

derived = (
    grouped
    .select(
        "quality",
        "distinct_qualitys",
        "min_duration_min",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("last_session_start")).alias("days_since_last_session_start"),
        F.date_trunc("week", F.col("last_session_start")).alias("last_session_start_week"),
        F.least(F.col("distinct_qualitys"), F.col("n_quality_est")).alias("least_distinct_qualitys_n_quality_est"),
    )
)

result = (
    derived
    .select(
        "quality",
        "distinct_qualitys",
        "min_duration_min",
        "last_session_start_week",
        "least_distinct_qualitys_n_quality_est",
        (F.col("distinct_qualitys") + F.col("min_duration_min")).alias("distinct_qualitys_plus_min_duration_min"),
        F.date_trunc("hour", F.col("last_session_start_week")).alias("last_session_start_week_hour"),
    )
    .orderBy("distinct_qualitys_plus_min_duration_min", F.col("quality").desc())
)
