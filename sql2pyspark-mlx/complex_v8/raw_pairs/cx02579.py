from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_actor_event_id = Window.partitionBy("event_actor", "event_id")

scoped_events = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) | (F.col("actor") == "customer"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_activity = (
    scoped_events.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (~F.col("t.actor").isin("agent", "system")), "left_anti")
)

annotated = (
    with_activity
    .filter(~F.col("event_type").isin("assigned", "created"))
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.substring(F.trim(F.col("event_type")), 1, 4).alias("event_type_prefix"),
    )
)

line_level_events = (
    annotated.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.days_since_event_at",
        "s.event_type_prefix",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

stats = (
    line_level_events
    .groupBy(
        F.date_trunc("month", "event_event_at").alias("event_event_at_month"),
        "event_actor",
        "event_id",
    )
    .agg(
        F.sum(F.when(F.col("event_actor").isin("system", "agent"), F.col("days_since_event_at"))).alias("days_since_event_at_event_actor_in_system_agent"),
        F.max("days_since_event_at").alias("max_days_since_event_at"),
    )
)

ranked = (
    stats
    .select(
        "event_event_at_month",
        "event_actor",
        "event_id",
        "max_days_since_event_at",
        F.min("max_days_since_event_at").over(w_event_actor_event_id).alias("event_actor_event_id_min_max_days_since_event_at"),
        F.sum("max_days_since_event_at").over(w_event_actor_event_id).alias("event_actor_event_id_sum_max_days_since_event_at"),
    )
)

result = (
    ranked
    .select(
        "event_event_at_month",
        "event_actor",
        "event_actor_event_id_sum_max_days_since_event_at",
        F.date_format(F.col("event_event_at_month"), "yyyy-MM").alias("event_event_at_month_label"),
        (F.floor(F.col("max_days_since_event_at") / 100) * 100).alias("max_days_since_event_at_bucket"),
    )
    .orderBy(
        "event_actor",
        F.col("event_actor_event_id_sum_max_days_since_event_at").desc(),
    )
)
