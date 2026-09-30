from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_priority_count_csat = Window.partitionBy("ticket_priority_count", "csat")
w_channel_by_csat = Window.partitionBy("channel").orderBy(F.col("csat").desc(), "id")
w_all_by_opened_at = Window.orderBy("opened_at", "id")
w_agent_id_last_ticket_opened = Window.partitionBy("agent_id", F.to_date("last_ticket_opened_at")).orderBy("last_ticket_opened_at", "id")

filtered_tickets = (
    df_tickets
    .filter((F.col("csat") < 5) & (~F.col("channel").isin("chat", "email")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
        F.concat_ws(": ", F.col("priority"), F.col("channel")).alias("priority_channel_label"),
        F.upper(F.trim(F.col("priority"))).alias("priority_clean"),
    )
)

with_activity = (
    filtered_tickets.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left_semi")
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.max("opened_at").alias("last_ticket_opened_at"),
        F.count(F.when(~F.col("priority").isin("P2", "P4"), 1)).alias("ticket_priority_count"),
    )
)

with_counts = (
    with_activity.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "t.last_ticket_opened_at",
        F.coalesce(F.col("t.ticket_priority_count"), F.lit(0)).alias("ticket_priority_count"),
    )
)

ranked = (
    with_counts
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "tags",
        "priority_slug",
        "priority_clean",
        "last_ticket_opened_at",
        "ticket_priority_count",
        F.sum("csat").over(w_ticket_priority_count_csat).alias("ticket_priority_count_csat_sum_csat"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_ticket_priority_count_csat), F.lit(0)), 4).alias("csat_share_of_ticket_priority_count_csat"),
        F.max("requester_id").over(w_ticket_priority_count_csat).alias("ticket_priority_count_csat_max_requester_id"),
        F.round((F.unix_timestamp("opened_at") - F.unix_timestamp(F.lag("opened_at").over(w_channel_by_csat))) / 60, 1).alias("minutes_since_prev_opened_at"),
        F.ntile(4).over(w_all_by_opened_at).alias("opened_at_quartile"),
    )
)

priority_stats = (
    ranked
    .groupBy("priority")
    .agg(F.avg("opened_at_quartile").alias("avg_opened_at_quartile"))
)

compared = (
    ranked.alias("s")
    .join(priority_stats.alias("g"), F.col("g.priority") == F.col("s.priority"), "inner")
    .filter(F.col("s.opened_at_quartile") > F.col("g.avg_opened_at_quartile"))
    .select(
        "s.*",
        "g.avg_opened_at_quartile",
        F.round(F.col("s.opened_at_quartile") - F.col("g.avg_opened_at_quartile"), 2).alias("opened_at_quartile_minus_avg"),
    )
)

with_trends = (
    compared
    .select(
        "id",
        "requester_id",
        "opened_at",
        "priority",
        "tags",
        "priority_slug",
        "last_ticket_opened_at",
        "csat_share_of_ticket_priority_count_csat",
        "ticket_priority_count_csat_max_requester_id",
        "opened_at_quartile",
        "avg_opened_at_quartile",
        "opened_at_quartile_minus_avg",
        F.round((F.unix_timestamp("opened_at") - F.unix_timestamp(F.lag("opened_at").over(w_agent_id_last_ticket_opened))) / 60, 1).alias("minutes_since_prev_opened_at_2"),
        F.ntile(5).over(w_agent_id_last_ticket_opened).alias("last_ticket_opened_at_quintile"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "requester_id",
        "opened_at",
        "priority",
        "priority_slug",
        "last_ticket_opened_at",
        "ticket_priority_count_csat_max_requester_id",
        "minutes_since_prev_opened_at_2",
        "last_ticket_opened_at_quintile",
        F.abs(F.col("opened_at_quartile") - 6).alias("opened_at_quartile_distance"),
        F.concat(F.col("priority"), F.lit("#"), F.col("id").cast("string")).alias("priority_ref"),
    )
    .orderBy("priority", "opened_at")
)
