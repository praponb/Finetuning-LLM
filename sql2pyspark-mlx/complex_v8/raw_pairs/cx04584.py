from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_len_by_csat_full = Window.partitionBy("channel_len").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_opened_at_year_csat = Window.partitionBy(F.year("opened_at"), "csat")
w_channel_len_by_requester_id = Window.partitionBy("channel_len").orderBy("requester_id", "id")
w_csat_by_channel_len_full = Window.partitionBy("csat").orderBy(F.col("channel_len").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_tickets = (
    df_tickets
    .filter(F.col("agent_id") >= 7)
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
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
    )
)

with_parent = (
    prepared_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.priority_slug",
        "s.channel_len",
        F.col("su.tier").alias("agent_tier"),
    )
)

windowed = (
    with_parent
    .select(
        "*",
        F.nth_value("closed_at", 2).over(w_channel_len_by_csat_full).alias("second_closed_at"),
        F.round(F.avg("csat").over(w_opened_at_year_csat), 2).alias("opened_at_year_csat_avg_csat"),
        F.lag("opened_at").over(w_channel_len_by_requester_id).alias("prev_opened_at"),
        F.round(F.col("channel_len") - F.avg("channel_len").over(w_opened_at_year_csat), 2).alias("channel_len_vs_opened_at_year_csat_avg"),
        F.last("csat").over(w_csat_by_channel_len_full).alias("last_csat"),
        F.count(F.lit(1)).over(w_opened_at_year_csat).alias("rows_in_opened_at_year_csat"),
        F.size(F.collect_set("agent_id").over(w_opened_at_year_csat)).alias("distinct_agent_id_in_opened_at_year_csat"),
        F.sum("channel_len").over(w_opened_at_year_csat).alias("opened_at_year_csat_sum_channel_len"),
    )
)

result = (
    windowed
    .filter(F.col("requester_id") < 20)
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        "priority_slug",
        "channel_len",
        "agent_tier",
        "second_closed_at",
        "opened_at_year_csat_avg_csat",
        "channel_len_vs_opened_at_year_csat_avg",
        "last_csat",
        "rows_in_opened_at_year_csat",
        "distinct_agent_id_in_opened_at_year_csat",
        "opened_at_year_csat_sum_channel_len",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("prev_opened_at")).alias("days_since_prev_opened_at"),
        F.year(F.col("second_closed_at")).alias("second_closed_at_year"),
    )
    .orderBy("opened_at_year_csat_avg_csat", F.col("last_csat").desc(), "id")
    .limit(20)
)
