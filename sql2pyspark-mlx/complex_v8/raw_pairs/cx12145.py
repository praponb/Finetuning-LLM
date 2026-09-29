from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_running = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
)

with_prev = prepared.withColumn("prev_event_at", F.lag("event_at").over(w_event_type))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_event_type_running))

per_session = (
    sessionized
    .groupBy("event_type", "burst_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("event_type_len").alias("session_event_type_len"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

grouped = (
    per_session
    .groupBy("event_type")
    .agg(
        F.sum(F.when(F.col("event_type") != "escalated", F.col("duration_min"))).alias("duration_min_event_type_escalated"),
        F.sum("duration_min").alias("total_duration_min"),
        F.percentile_approx("burst_id", 0.5).alias("median_burst_id"),
        F.count("*").alias("n_rows"),
        F.sum("session_event_type_len").alias("sum_session_event_type_len_check"),
    )
    .filter(F.col("sum_session_event_type_len_check") > 24)
    .drop("sum_session_event_type_len_check")
)

screened = (
    grouped
    .crossJoin(grouped.agg(F.avg("total_duration_min").alias("overall_avg_total_duration_min")))
    .filter(F.col("total_duration_min") > F.col("overall_avg_total_duration_min"))
    .drop("overall_avg_total_duration_min")
)

result = (
    screened
    .select(
        "event_type",
        "total_duration_min",
        "n_rows",
        F.coalesce(F.col("n_rows"), F.lit(0)).alias("n_rows_filled"),
    )
    .orderBy(F.col("event_type").desc())
)
