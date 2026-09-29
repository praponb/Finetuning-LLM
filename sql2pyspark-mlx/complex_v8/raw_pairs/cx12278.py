from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_priority_event_at_wee = Window.partitionBy("ticket_priority", F.year("event_at_week")).orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at_ties = Window.orderBy("event_at")
w_ticket_channel_by_event_at_w = Window.partitionBy("ticket_channel").orderBy("event_at_week", "id")
w_all_by_event_at_week = Window.orderBy("event_at_week", "id")
w_actor_by_event_at = Window.partitionBy("actor").orderBy("event_at", "id")
w_all_by_event_at_running = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_events = (
    df_ticket_events
    .filter(F.col("actor").isin("customer", "agent"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("week", F.col("event_at")).alias("event_at_week"),
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label"),
    )
)

joined_ticket = (
    valid_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.*",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.tags").alias("ticket_tags"),
        "su.agent_name",
    )
)

with_trends = (
    joined_ticket
    .select(
        "*",
        F.first("event_at", ignorenulls=True).over(w_ticket_priority_event_at_wee).alias("first_known_event_at"),
        F.round(F.cume_dist().over(w_all_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.lead("event_at_week", 3).over(w_ticket_channel_by_event_at_w).alias("next3_event_at_week"),
        F.round((F.unix_timestamp("event_at_week") - F.unix_timestamp(F.lag("event_at_week").over(w_all_by_event_at_week))) / 60, 1).alias("minutes_since_prev_event_at_week"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_all_by_event_at_week))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.round(F.cume_dist().over(w_actor_by_event_at), 3).alias("event_at_cume_dist_2"),
        F.last("event_at_week", ignorenulls=True).over(w_all_by_event_at_running).alias("last_known_event_at_week"),
        F.round(F.percent_rank().over(w_actor_by_event_at), 3).alias("event_at_percent_rank"),
        F.lead("event_at_week", 3).over(w_all_by_event_at_week).alias("next3_event_at_week_2"),
        F.rank().over(w_all_by_event_at_week).alias("event_at_week_rank"),
        F.lead("event_at").over(w_all_by_event_at_week).alias("next_event_at"),
        F.nth_value("event_at", 2).over(w_all_by_event_at_full).alias("second_event_at"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "event_at",
        "event_at_week",
        "event_at_label",
        "ticket_channel",
        "ticket_tags",
        "first_known_event_at",
        "event_at_cume_dist",
        "next3_event_at_week",
        "minutes_since_prev_event_at",
        "event_at_cume_dist_2",
        "next3_event_at_week_2",
        "next_event_at",
        "second_event_at",
        F.round(F.col("event_at_cume_dist") / F.nullif(F.col("event_at_week_rank"), F.lit(0)), 3).alias("event_at_cume_dist_per_event_at_week_rank"),
    )
    .orderBy(F.col("next_event_at").desc(), F.col("second_event_at").desc())
)
