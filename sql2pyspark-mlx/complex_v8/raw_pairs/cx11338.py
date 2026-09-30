from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_tier_priority_by_ticke = Window.partitionBy("agent_tier", "priority").orderBy(F.col("ticket_channel_count").desc_nulls_first())
w_priority = Window.partitionBy("priority")
w_agent_id_ticket_channel_coun = Window.partitionBy("agent_id", "ticket_channel_count").orderBy(F.col("closed_at").desc_nulls_first(), "id", "tag").rowsBetween(-1, -1)
w_all_by_ticket_channel_count = Window.orderBy("ticket_channel_count", "id", "tag").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_tickets = (
    df_tickets
    .filter((F.hour(F.col("opened_at")).between(10, 12)) & (F.col("channel").isin("phone", "chat", "email")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
        "tags",
        F.date_format(F.col("closed_at"), "yyyy-MM").alias("closed_at_label"),
        F.concat(F.col("priority"), F.lit("#"), F.col("csat").cast("string")).alias("priority_ref"),
    )
)

screened = (
    filtered_tickets.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.actor") == "customer") & (F.to_date(F.col("t.event_at")) < F.lit("2025-03-08").cast("date")), "left_anti")
)

combined_details = (
    screened.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.tier") == "T1")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.csat",
        "s.tags",
        "s.closed_at_label",
        "s.priority_ref",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

exploded = (
    combined_details
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
        "closed_at_label",
        "priority_ref",
        "agent_tier",
        "agent_team",
        F.explode("tags").alias("tag"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.min("csat").alias("min_ticket_csat"),
        F.count(F.when(F.col("channel").isin("email", "web"), 1)).alias("ticket_channel_count"),
    )
)

summarized_rows = (
    exploded.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "t.min_ticket_csat",
        F.coalesce(F.col("t.ticket_channel_count"), F.lit(0)).alias("ticket_channel_count"),
    )
)

sequenced = (
    summarized_rows
    .select(
        "id",
        "requester_id",
        "closed_at",
        "priority",
        "csat",
        "closed_at_label",
        "priority_ref",
        "agent_tier",
        "tag",
        "ticket_channel_count",
        F.dense_rank().over(w_agent_tier_priority_by_ticke).alias("ticket_channel_count_dense_rank"),
        F.size(F.collect_set("agent_tier").over(w_priority)).alias("distinct_agent_tier_in_priority"),
        F.sum("ticket_channel_count").over(w_agent_id_ticket_channel_coun).alias("ticket_channel_count_sum_1_prior"),
        F.round(F.col("ticket_channel_count") / F.nullif(F.sum("ticket_channel_count").over(w_priority), F.lit(0)), 4).alias("ticket_channel_count_share_of_priority"),
        F.nth_value("csat", 2).over(w_all_by_ticket_channel_count).alias("second_csat"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "closed_at",
        "priority",
        "csat",
        "closed_at_label",
        "priority_ref",
        "agent_tier",
        "tag",
        "ticket_channel_count",
        "ticket_channel_count_dense_rank",
        "distinct_agent_tier_in_priority",
        "ticket_channel_count_sum_1_prior",
        "second_csat",
        F.substring(F.trim(F.col("agent_tier")), 1, 4).alias("agent_tier_prefix"),
        F.concat_ws("-", F.col("priority"), F.col("closed_at_label")).alias("priority_closed_at_label_label"),
    )
    .orderBy("csat")
)
