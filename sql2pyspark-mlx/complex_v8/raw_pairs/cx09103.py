from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_csat_filled = Window.orderBy(F.col("csat_filled").desc_nulls_first(), "id")
w_channel_csat_filled = Window.partitionBy("channel", "csat_filled")

base_tickets = (
    df_tickets
    .filter((F.hour(F.col("opened_at")).between(7, 12)) & (F.col("requester_id") <= 17))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
        F.month(F.col("opened_at")).alias("opened_at_month"),
    )
)

ranked = (
    base_tickets
    .select(
        "id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        "opened_at_month",
        F.round(F.cume_dist().over(w_all_by_csat_filled), 3).alias("csat_filled_cume_dist"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_channel_csat_filled), F.lit(0)), 4).alias("csat_share_of_channel_csat_filled"),
        F.lag("opened_at_month").over(w_all_by_csat_filled).alias("prev_opened_at_month"),
        F.round(F.avg("csat").over(w_channel_csat_filled), 2).alias("channel_csat_filled_avg_csat"),
    )
)

screened = (
    ranked.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.hour(F.col("t.event_at")).between(8, 11)), "left_anti")
)

without_matches = (
    screened.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left_semi")
)

screened_2 = (
    without_matches
    .crossJoin(without_matches.agg(F.avg("opened_at_month").alias("overall_avg_opened_at_month")))
    .filter(F.col("opened_at_month") > F.col("overall_avg_opened_at_month"))
    .drop("overall_avg_opened_at_month")
)

line_level = (
    screened_2.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.csat",
        "s.prev_opened_at_month",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_type",
        "t.event_at",
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(F.col("event_type") == "reopened", 1)).alias("event_event_type_count"),
    )
)

summarized_rows = (
    line_level.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
    )
)

result = (
    summarized_rows
    .groupingSets(
        [["event_type", "event_event_type_count"], ["event_type"], []],
        "event_type", "event_event_type_count",
    )
    .agg(
        F.round(F.avg("prev_opened_at_month"), 2).alias("avg_prev_opened_at_month"),
    )
    .select(
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "event_event_type_count",
        "avg_prev_opened_at_month",
    )
    .orderBy(
        F.col("event_type").asc_nulls_last(),
        F.col("event_event_type_count").asc_nulls_last(),
    )
)
