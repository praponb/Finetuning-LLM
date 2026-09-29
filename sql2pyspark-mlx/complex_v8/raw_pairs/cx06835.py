from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel")
w_agent_tier = Window.partitionBy("agent_tier")
w_all_by_opened_at_ties = Window.orderBy(F.col("opened_at").desc())
w_all_by_opened_at = Window.orderBy(F.col("opened_at").desc(), "id")
w_agent_id_opened_at_to_date_b = Window.partitionBy("agent_id", F.to_date("opened_at")).orderBy(F.col("csat").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_median_agent_tier_sum = Window.orderBy("median_agent_tier_sum_csat")
w_all_by_median_agent_tier_sum_2 = Window.orderBy("median_agent_tier_sum_csat", "opened_at_dense_rank")

prepared = (
    df_tickets
    .filter((~F.col("priority").isin("P4", "P2")) & (F.col("agent_id").isNotNull()))
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
        F.substring(F.trim(F.col("channel")), 1, 4).alias("channel_prefix"),
    )
)

joined_agent = (
    prepared.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select("s.*", F.col("su.tier").alias("agent_tier"))
)

windowed = (
    joined_agent
    .select(
        "*",
        F.size(F.collect_set("priority").over(w_channel)).alias("distinct_priority_in_channel"),
        F.sum("csat").over(w_agent_tier).alias("agent_tier_sum_csat"),
        F.dense_rank().over(w_all_by_opened_at_ties).alias("opened_at_dense_rank"),
        F.max("requester_id").over(w_agent_tier).alias("agent_tier_max_requester_id"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_agent_tier), F.lit(0)), 4).alias("requester_id_share_of_agent_tier"),
        F.round(F.col("csat") - F.avg("csat").over(w_channel), 2).alias("csat_vs_channel_avg"),
        F.rank().over(w_all_by_opened_at).alias("opened_at_rank"),
        F.count(F.lit(1)).over(w_channel).alias("rows_in_channel"),
        F.max("requester_id").over(w_agent_id_opened_at_to_date_b).alias("requester_id_max_2"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_channel), F.lit(0)), 4).alias("requester_id_share_of_channel"),
    )
)

first_rows = (
    windowed
    .filter(F.col("opened_at_rank") <= 5)
    .drop("opened_at_rank")
)

per_group = (
    first_rows
    .groupBy("opened_at_dense_rank")
    .agg(
        F.array_sort(F.collect_set("agent_tier")).alias("agent_tier_list"),
        F.percentile_approx("agent_tier_sum_csat", 0.5).alias("median_agent_tier_sum_csat"),
    )
)

windowed_2 = (
    per_group
    .select(
        "opened_at_dense_rank",
        "agent_tier_list",
        F.round(F.percent_rank().over(w_all_by_median_agent_tier_sum), 3).alias("median_agent_tier_sum_csat_percent_rank"),
        (F.col("median_agent_tier_sum_csat") - F.lag("median_agent_tier_sum_csat").over(w_all_by_median_agent_tier_sum_2)).alias("median_agent_tier_sum_csat_change"),
    )
)

result = (
    windowed_2
    .select(
        "opened_at_dense_rank",
        F.abs(F.col("opened_at_dense_rank") - 6).alias("opened_at_dense_rank_distance"),
    )
    .orderBy(F.col("opened_at_dense_rank").desc(), "opened_at_dense_rank_distance")
)
