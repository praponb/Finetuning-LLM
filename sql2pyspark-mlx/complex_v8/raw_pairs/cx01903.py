from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_by_duration_min_runn = Window.partitionBy("quality").orderBy(F.col("duration_min").desc(), "quality", "burst_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_burst_id = Window.partitionBy("quality", "burst_id")
w_quality_by_duration_min_full = Window.partitionBy("quality").orderBy(F.col("duration_min").desc(), "quality", "burst_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_session_start_month_burst_id = Window.partitionBy(F.date_trunc("month", "session_start"), "burst_id").orderBy(F.col("duration_min").asc_nulls_last())

recent_readings = (
    df_meter_readings
    .filter(F.col("kwh") > 37.1)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 3).alias("quality_prefix"),
    )
)

with_prev = recent_readings.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_quality_running))

per_session = (
    sessionized
    .groupBy("quality", "burst_id")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

with_trends = (
    per_session
    .select(
        "*",
        F.count(F.lit(1)).over(w_quality_by_duration_min_runn).alias("running_count"),
        F.round(F.col("n_events") / F.nullif(F.sum("n_events").over(w_quality_burst_id), F.lit(0)), 4).alias("n_events_share_of_quality_burst_id"),
        F.first("n_events").over(w_quality_by_duration_min_full).alias("first_n_events"),
        F.round(F.col("burst_id") / F.nullif(F.sum("burst_id").over(w_quality_burst_id), F.lit(0)), 4).alias("burst_id_share_of_quality_burst_id"),
        F.rank().over(w_session_start_month_burst_id).alias("duration_min_rank"),
    )
)

leaders = (
    with_trends
    .filter(F.col("duration_min_rank") == 1)
    .drop("duration_min_rank")
)

result = (
    leaders
    .groupBy("burst_id", "session_end")
    .pivot("quality", ["OK", "EST", "BAD"])
    .agg(F.count("running_count"))
)
