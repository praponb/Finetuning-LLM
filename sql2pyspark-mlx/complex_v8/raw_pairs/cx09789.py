from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_group_by_event_at_full = Window.partitionBy("actor_group").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at = Window.orderBy("event_at", "id")

cleaned_events = (
    df_ticket_events
    .filter((F.col("event_at") >= F.lit("2025-02-06 10:00:00").cast("timestamp")) & (F.col("event_type").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("actor").isin("system", "agent"), "group_a").when(F.col("actor") == "customer", "group_b").otherwise("other").alias("actor_group"),
        (F.col("event_at") + F.expr("INTERVAL 15 MINUTES")).alias("event_at_plus_15m"),
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label"),
    )
)

sequenced = (
    cleaned_events
    .withColumn("first_event_at_plus_15m", F.first("event_at_plus_15m").over(w_actor_group_by_event_at_full))
    .withColumn("event_at_percent_rank", F.round(F.percent_rank().over(w_all_by_event_at), 3))
)

qualified = (
    sequenced.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_at") >= F.lit("2025-01-25 08:00:00").cast("timestamp")) & (F.col("t.event_type").isin("reopened", "escalated")), "left_anti")
)

scored = (
    qualified
    .select(
        "*",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at_plus_15m")).alias("days_since_event_at_plus_15m"),
        F.concat_ws("-", F.col("event_type"), F.col("event_at_label")).alias("event_type_event_at_label_label"),
    )
)

with_children_events = (
    scored.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor") == "system"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor_group",
        "s.event_at_label",
        "s.first_event_at_plus_15m",
        "s.days_since_event_at_plus_15m",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.actor").alias("event_actor"),
    )
)

with_activity = (
    with_children_events.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.id") == F.col("s.event_id")) & (F.hour(F.col("t.event_at")).between(8, 10)), "left_semi")
)

result = (
    with_activity
    .filter(F.col("event_id") >= 32)
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor_group",
        "event_at_label",
        "first_event_at_plus_15m",
        "days_since_event_at_plus_15m",
        "event_id",
        "event_event_type",
        "event_actor",
    )
    .orderBy(
        F.col("id").asc_nulls_last(),
        F.col("days_since_event_at_plus_15m").asc(),
    )
)
