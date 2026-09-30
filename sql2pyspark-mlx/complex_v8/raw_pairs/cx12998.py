from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_tier_by_opened_at = Window.partitionBy("agent_tier").orderBy(F.col("opened_at").desc_nulls_first(), "id")
w_agent_id_channel_by_opened_a = Window.partitionBy("agent_id", "channel").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_opened_at_month_csat = Window.partitionBy(F.date_trunc("month", "opened_at"), "csat")
w_agent_id_channel_by_opened_a_2 = Window.partitionBy("agent_id", "channel").orderBy("opened_at")
w_agent_tier_by_opened_at_runn = Window.partitionBy("agent_tier").orderBy(F.col("opened_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_channel_or_unknown_by_opened = Window.partitionBy("channel_or_unknown").orderBy(F.col("opened_at").asc_nulls_last(), "id").rowsBetween(-3, -1)

valid_tickets = (
    df_tickets
    .filter(F.col("priority").isNotNull())
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
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled"),
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
    )
)

with_details = (
    valid_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.channel_or_unknown",
        F.col("su.tier").alias("agent_tier"),
    )
)

with_trends = (
    with_details
    .select(
        "id",
        "requester_id",
        "agent_id",
        "channel",
        "csat",
        "agent_tier",
        F.row_number().over(w_agent_tier_by_opened_at).alias("opened_at_seq"),
        F.min("requester_id").over(w_agent_id_channel_by_opened_a).alias("min_requester_id_so_far"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_opened_at_month_csat), F.lit(0)), 4).alias("requester_id_share_of_opened_at_month_csat"),
        F.round(F.col("csat") - F.avg("csat").over(w_opened_at_month_csat), 2).alias("csat_vs_opened_at_month_csat_avg"),
        F.count(F.lit(1)).over(w_opened_at_month_csat).alias("rows_in_opened_at_month_csat"),
        F.max("requester_id").over(w_opened_at_month_csat).alias("opened_at_month_csat_max_requester_id"),
        F.dense_rank().over(w_agent_id_channel_by_opened_a_2).alias("opened_at_dense_rank"),
        F.last("closed_at", ignorenulls=True).over(w_agent_tier_by_opened_at_runn).alias("last_known_closed_at"),
        F.sum("requester_id").over(w_agent_tier_by_opened_at_runn).alias("running_requester_id"),
        F.round(F.avg("csat").over(w_opened_at_month_csat), 2).alias("opened_at_month_csat_avg_csat"),
        F.sum("requester_id").over(w_opened_at_month_csat).alias("opened_at_month_csat_sum_requester_id"),
        F.round(F.avg("csat").over(w_channel_or_unknown_by_opened), 2).alias("csat_avg_3_prior"),
    )
)

per_group = (
    with_trends
    .groupBy(
        F.date_trunc("month", "last_known_closed_at").alias("last_known_closed_at_month"),
        "agent_tier",
    )
    .agg(
        F.percentile_approx("csat", 0.5).alias("median_csat"),
        F.array_sort(F.collect_set("agent_tier")).alias("agent_tier_list"),
        F.count("*").alias("cnt"),
        F.max("opened_at_month_csat_max_requester_id").alias("max_opened_at_month_csat_max_requester_id_check"),
    )
    .filter(F.col("max_opened_at_month_csat_max_requester_id_check") > 26)
    .drop("max_opened_at_month_csat_max_requester_id_check")
)

result = (
    per_group
    .select(
        "last_known_closed_at_month",
        "agent_tier",
        "median_csat",
        "agent_tier_list",
        "cnt",
        F.coalesce(F.col("agent_tier"), F.lit("UNKNOWN")).alias("agent_tier_or_unknown"),
        F.abs(F.col("cnt") - 5).alias("cnt_distance"),
    )
    .orderBy(F.col("cnt_distance").desc())
)
