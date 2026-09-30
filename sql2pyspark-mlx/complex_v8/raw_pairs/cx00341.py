from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        "tags",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
    )
)

with_prev = recent_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_agent_id_running))

session_summary = (
    sessionized
    .groupBy("agent_id", "session_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("days_since_opened_at").alias("session_days_since_opened_at"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

per_group = (
    session_summary
    .filter(F.col("agent_id").isNotNull())
    .groupBy(F.date_trunc("month", "session_end").alias("session_end_month"))
    .agg(
        F.min("session_start").alias("first_session_start"),
        F.round(F.sum("session_days_since_opened_at") / F.nullif(F.sum("n_events"), F.lit(0)), 3).alias("session_days_since_opened_at_to_n_events_ratio"),
        F.avg("duration_min").alias("avg_duration_min_check"),
    )
    .filter(F.col("avg_duration_min_check") > 72.0)
    .drop("avg_duration_min_check")
)

result = (
    per_group
    .select(
        "session_end_month",
        "first_session_start",
        "session_days_since_opened_at_to_n_events_ratio",
        F.round((F.unix_timestamp(F.col("first_session_start")) - F.unix_timestamp(F.col("session_end_month"))) / 60, 1).alias("minutes_session_end_month_to_first_session_start"),
        F.to_date(F.col("session_end_month")).alias("session_end_month_date"),
    )
    .orderBy("first_session_start")
)
