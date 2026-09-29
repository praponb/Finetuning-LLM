from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_csat_ties = Window.orderBy(F.col("csat").desc())
w_agent_tier = Window.partitionBy("agent_tier")
w_agent_id_csat_by_csat_ties = Window.partitionBy("agent_id", "csat").orderBy(F.col("csat").desc_nulls_first())
w_agent_id_csat_by_csat_full = Window.partitionBy("agent_id", "csat").orderBy(F.col("csat").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_priority = Window.partitionBy("priority")
w_csat_channel_by_csat_ties = Window.partitionBy("csat", "channel").orderBy(F.col("csat").desc())
w_csat_channel_by_csat = Window.partitionBy("csat", "channel").orderBy(F.col("csat").desc(), "id")
w_all_by_csat = Window.orderBy(F.col("csat").desc(), "id")
w_agent_id_csat_by_csat = Window.partitionBy("agent_id", "csat").orderBy(F.col("csat").desc_nulls_first(), "id")
w_all_by_csat_full = Window.orderBy(F.col("csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_csat_dense_rank = Window.partitionBy("agent_id", "csat_dense_rank")
w_csat_dense_rank_by_csat_cume = Window.partitionBy("csat_dense_rank").orderBy("csat_cume_dist_2")
w_csat_rank_by_second_csat_2_r = Window.partitionBy("csat_rank").orderBy(F.col("second_csat_2").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_csat_dense_rank_by_csat_cume_2 = Window.partitionBy("csat_dense_rank").orderBy("csat_cume_dist_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_tickets = (
    df_tickets
    .filter(F.col("priority") == "P2")
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "csat",
    )
)

joined = (
    base_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select("s.*", F.col("su.tier").alias("agent_tier"))
)

metrics = (
    joined
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_csat_ties), 3).alias("csat_percent_rank"),
        F.round(F.col("csat") - F.avg("csat").over(w_agent_tier), 2).alias("csat_vs_agent_tier_avg"),
        F.round(F.percent_rank().over(w_agent_id_csat_by_csat_ties), 3).alias("csat_percent_rank_2"),
        F.nth_value("csat", 2).over(w_agent_id_csat_by_csat_full).alias("second_csat"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_priority), F.lit(0)), 4).alias("csat_share_of_priority"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_agent_tier), F.lit(0)), 4).alias("csat_share_of_agent_tier"),
        F.rank().over(w_all_by_csat_ties).alias("csat_rank"),
        F.dense_rank().over(w_csat_channel_by_csat_ties).alias("csat_dense_rank"),
        (F.col("csat") - F.lag("csat").over(w_csat_channel_by_csat)).alias("csat_change"),
        F.round(F.cume_dist().over(w_agent_id_csat_by_csat_ties), 3).alias("csat_cume_dist"),
        F.lead("csat", 2).over(w_all_by_csat).alias("next2_csat"),
        F.count(F.lit(1)).over(w_agent_tier).alias("rows_in_agent_tier"),
        F.round(F.cume_dist().over(w_agent_id_csat_by_csat), 3).alias("csat_cume_dist_2"),
        F.nth_value("csat", 2).over(w_all_by_csat_full).alias("second_csat_2"),
    )
)

windowed = (
    metrics
    .select(
        "*",
        F.size(F.collect_set("agent_tier").over(w_agent_id_csat_dense_rank)).alias("distinct_agent_tier_in_agent_id_csat_dense_rank"),
        F.round(F.percent_rank().over(w_csat_dense_rank_by_csat_cume), 3).alias("csat_cume_dist_2_percent_rank"),
        F.min("csat_cume_dist_2").over(w_csat_rank_by_second_csat_2_r).alias("min_csat_cume_dist_2_so_far"),
        F.min("second_csat_2").over(w_csat_dense_rank_by_csat_cume_2).alias("min_second_csat_2_so_far"),
        F.round(F.col("csat_share_of_agent_tier") / F.nullif(F.sum("csat_share_of_agent_tier").over(w_agent_id_csat_dense_rank), F.lit(0)), 4).alias("csat_share_of_agent_tier_share_of_agent_id_csat_"),
    )
)

result = (
    windowed
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "csat",
        "agent_tier",
        "csat_percent_rank",
        "csat_vs_agent_tier_avg",
        "csat_percent_rank_2",
        "second_csat",
        "csat_share_of_priority",
        "csat_rank",
        "csat_dense_rank",
        "csat_change",
        "csat_cume_dist",
        "next2_csat",
        "rows_in_agent_tier",
        "csat_cume_dist_2",
        "second_csat_2",
        "csat_cume_dist_2_percent_rank",
        "min_csat_cume_dist_2_so_far",
        "min_second_csat_2_so_far",
        "csat_share_of_agent_tier_share_of_agent_id_csat_",
    )
    .orderBy("priority")
)
