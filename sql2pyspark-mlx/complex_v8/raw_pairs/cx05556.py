from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_running = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_start_to_date_sessio = Window.partitionBy(F.to_date("session_start"), "session_id")
w_event_type_by_event_count = Window.partitionBy("event_type").orderBy("event_count", "event_type", "session_id")
w_event_type_by_event_count_ti = Window.partitionBy("event_type").orderBy("event_count")
w_event_type_by_event_count_fu = Window.partitionBy("event_type").orderBy("event_count", "event_type", "session_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
)

with_prev = filtered.withColumn("prev_event_at", F.lag("event_at").over(w_event_type))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_event_type_running))

sessions = (
    sessionized
    .groupBy("event_type", "session_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

windowed = (
    sessions
    .select(
        "event_type",
        "session_id",
        "session_start",
        "event_count",
        "duration_min",
        F.round(F.col("session_id") - F.avg("session_id").over(w_session_start_to_date_sessio), 2).alias("session_id_vs_session_start_to_date_session_id_a"),
        F.dense_rank().over(w_event_type_by_event_count).alias("event_count_dense_rank"),
        F.round(F.cume_dist().over(w_event_type_by_event_count_ti), 3).alias("event_count_cume_dist"),
        F.round(F.percent_rank().over(w_event_type_by_event_count_ti), 3).alias("event_count_percent_rank"),
        F.nth_value("session_id", 2).over(w_event_type_by_event_count_fu).alias("second_session_id"),
    )
)

per_group = (
    windowed
    .groupBy(
        F.date_trunc("month", "session_start").alias("session_start_month"),
        "session_id",
        "event_count_dense_rank",
    )
    .agg(
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.count("*").alias("row_count"),
        F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
        F.max("event_count").alias("max_event_count"),
    )
)

result = (
    per_group
    .select(
        "session_start_month",
        "session_id",
        "event_count_dense_rank",
        "event_type_list",
        "max_event_count",
        F.array_join(F.array_sort(F.col("event_type_list")), ",").alias("event_type_list_text"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("session_start_month")).alias("days_since_session_start_month"),
    )
    .orderBy("event_count_dense_rank", "event_type_list_text")
)
