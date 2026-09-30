from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_ticket_events
    .filter(F.col("ticket_id") >= 54)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.hour(F.col("event_at")).alias("event_at_hour"),
    )
)

with_prev = base.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_actor_running))

session_summary = (
    sessionized
    .groupBy("actor", "session_no")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("event_at_hour").alias("session_event_at_hour"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

stats = (
    session_summary
    .groupBy(
        F.date_trunc("month", "session_start").alias("session_start_month"),
        "session_no",
    )
    .agg(
        F.max("session_no").alias("max_session_no"),
        F.round(F.sum("event_count") / F.nullif(F.sum("session_no"), F.lit(0)), 3).alias("event_count_to_session_no_ratio"),
        F.sum(F.when(F.col("actor") == "agent", 1).otherwise(0)).alias("n_actor_agent"),
        F.round(F.avg("session_event_at_hour"), 2).alias("avg_session_event_at_hour"),
        F.sum("duration_min").alias("total_duration_min"),
        F.min("session_event_at_hour").alias("min_session_event_at_hour"),
    )
)

enriched = (
    stats
    .select(
        "*",
        F.when(F.col("n_actor_agent") >= 7, "A").when(F.col("n_actor_agent") >= 4, "B").otherwise("C").alias("n_actor_agent_tier"),
        F.round(F.col("total_duration_min") * 1.1, 2).alias("total_duration_min_adj"),
    )
)

result = (
    enriched
    .filter(F.col("n_actor_agent") <= 8)
    .select(
        "session_start_month",
        "session_no",
        "n_actor_agent",
        "avg_session_event_at_hour",
        "total_duration_min",
        "min_session_event_at_hour",
        "n_actor_agent_tier",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("session_start_month")).alias("days_since_session_start_month"),
    )
)
