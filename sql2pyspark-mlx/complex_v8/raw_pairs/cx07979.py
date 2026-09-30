from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_channel_by_days_since = Window.partitionBy("ticket_channel").orderBy(F.col("days_since_event_at").desc_nulls_first(), "id").rowsBetween(-1, -1)
w_event_type_actor = Window.partitionBy("event_type", "actor")
w_ticket_channel_by_days_since_2 = Window.partitionBy("ticket_channel").orderBy(F.col("days_since_event_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_channel_by_days_since_3 = Window.partitionBy("ticket_channel").orderBy(F.col("days_since_event_at").desc_nulls_first())

cleaned = (
    df_ticket_events
    .filter(~F.col("event_type").isin("resolved", "created"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.to_date(F.col("event_at")).alias("event_at_date"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

with_details_ticket = (
    cleaned.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.days_since_event_at",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

metrics = (
    with_details_ticket
    .select(
        "*",
        F.max("days_since_event_at").over(w_ticket_channel_by_days_since).alias("days_since_event_at_max_1_prior"),
        F.round(F.col("days_since_event_at") / F.nullif(F.sum("days_since_event_at").over(w_event_type_actor), F.lit(0)), 4).alias("days_since_event_at_share_of_event_type_actor"),
        F.first("ticket_opened_at").over(w_ticket_channel_by_days_since_2).alias("first_ticket_opened_at"),
        F.round(F.avg("days_since_event_at").over(w_event_type_actor), 2).alias("event_type_actor_avg_days_since_event_at"),
        F.rank().over(w_ticket_channel_by_days_since_3).alias("days_since_event_at_rank"),
        F.max("days_since_event_at").over(w_event_type_actor).alias("event_type_actor_max_days_since_event_at"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.min("event_at").alias("first_event_at"))
)

with_stats = (
    metrics.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.first_event_at")
)

result = (
    with_stats
    .cube("ticket_channel", "ticket_id", "days_since_event_at_rank")
    .agg(
        F.min("first_event_at").alias("first_first_event_at"),
        F.sum("event_type_actor_max_days_since_event_at").alias("total_event_type_actor_max_days_since_event_at"),
    )
    .select(
        F.coalesce(F.col("ticket_channel"), F.lit("ALL")).alias("ticket_channel"),
        "ticket_id",
        "days_since_event_at_rank",
        "first_first_event_at",
        "total_event_type_actor_max_days_since_event_at",
    )
    .orderBy(
        F.col("ticket_channel").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
        F.col("days_since_event_at_rank").asc_nulls_last(),
    )
)
