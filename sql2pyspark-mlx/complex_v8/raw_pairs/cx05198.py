from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_count = Window.partitionBy("event_count")
w_session_start_to_date_by_ses = Window.partitionBy(F.to_date("session_start")).orderBy("session_end")
w_session_start_to_date_by_ses_2 = Window.partitionBy(F.to_date("session_start")).orderBy("session_end", "site_id", "burst_id").rowsBetween(-6, Window.currentRow)

scoped = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.col("read_at") + F.expr("INTERVAL 30 MINUTES")).alias("read_at_plus_30m"),
    )
)

with_prev = scoped.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_site_id_running))

sessions = (
    sessionized
    .groupBy("site_id", "burst_id")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

with_trends = (
    sessions
    .select(
        "*",
        F.round(F.avg("event_count").over(w_event_count), 2).alias("event_count_avg_event_count"),
        F.round(F.percent_rank().over(w_session_start_to_date_by_ses), 3).alias("session_end_percent_rank"),
        F.sum("duration_min").over(w_session_start_to_date_by_ses_2).alias("duration_min_sum_7"),
    )
)

labeled = (
    with_trends
    .select(
        "*",
        F.round((F.unix_timestamp(F.col("session_start")) - F.unix_timestamp(F.col("session_end"))) / 3600, 1).alias("hours_session_end_to_session_start"),
        F.round(F.col("duration_min_sum_7") * 1.07, 2).alias("duration_min_sum_7_adj"),
        F.when(F.col("session_end_percent_rank") >= 0.8, "HIGH").when(F.col("session_end_percent_rank") >= 0.3, "MEDIUM").otherwise("LOW").alias("session_end_percent_rank_tier"),
    )
)

result = (
    labeled
    .groupBy("burst_id")
    .pivot("session_end_percent_rank_tier", ["LOW", "HIGH"])
    .agg(F.sum("event_count").alias("amount"), F.max("event_count").alias("max_v"))
    .orderBy("burst_id")
)
