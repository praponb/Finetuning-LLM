from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_team_agent_tier = Window.partitionBy("agent_team", "agent_tier")
w_all_by_requester_id = Window.orderBy(F.col("requester_id").desc(), "id")
w_agent_id = Window.partitionBy("agent_id")
w_agent_tier_agent_id_by_reque = Window.partitionBy("agent_tier", "agent_id").orderBy(F.col("requester_id").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_by_csat = Window.partitionBy("csat").orderBy("csat", "id")
w_csat_by_csat_full = Window.partitionBy("csat").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_agent_tier_by_requester = Window.partitionBy("csat", "agent_tier").orderBy("requester_id")
w_agent_tier_agent_id_by_reque_2 = Window.partitionBy("agent_tier", "agent_id").orderBy(F.col("requester_id").asc_nulls_last(), "id").rowsBetween(-2, Window.currentRow)
w_second_csat_by_requester_id = Window.partitionBy("second_csat").orderBy("requester_id_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_agent_id_in_agent_t = Window.partitionBy("distinct_agent_id_in_agent_team_agent_tier", "agent_team")
w_all_by_next_csat = Window.orderBy(F.col("next_csat").desc_nulls_first(), "id")
w_second_csat_by_requester_id_2 = Window.partitionBy("second_csat").orderBy("requester_id_rank", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_next_csat_ties = Window.orderBy(F.col("next_csat").desc_nulls_first())

prepared_tickets = (
    df_tickets
    .filter(F.col("csat") > 1)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "channel",
        "csat",
    )
)

with_parent = (
    prepared_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.channel",
        "s.csat",
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
    )
)

ranked = (
    with_parent
    .select(
        "*",
        F.size(F.collect_set("channel").over(w_agent_team_agent_tier)).alias("distinct_channel_in_agent_team_agent_tier"),
        F.min("csat").over(w_agent_team_agent_tier).alias("agent_team_agent_tier_min_csat"),
        F.lead("csat").over(w_all_by_requester_id).alias("next_csat"),
        F.size(F.collect_set("channel").over(w_agent_id)).alias("distinct_channel_in_agent_id"),
        F.first("requester_id").over(w_agent_tier_agent_id_by_reque).alias("first_requester_id"),
        F.count(F.lit(1)).over(w_agent_team_agent_tier).alias("rows_in_agent_team_agent_tier"),
        F.lead("requester_id").over(w_csat_by_csat).alias("next_requester_id"),
        F.nth_value("csat", 2).over(w_csat_by_csat_full).alias("second_csat"),
        F.rank().over(w_csat_agent_tier_by_requester).alias("requester_id_rank"),
        F.size(F.collect_set("agent_id").over(w_agent_team_agent_tier)).alias("distinct_agent_id_in_agent_team_agent_tier"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_agent_team_agent_tier), F.lit(0)), 4).alias("csat_share_of_agent_team_agent_tier"),
        F.max("requester_id").over(w_agent_tier_agent_id_by_reque_2).alias("requester_id_max_3"),
    )
)

metrics = (
    ranked
    .select(
        "*",
        F.first("next_requester_id", ignorenulls=True).over(w_second_csat_by_requester_id).alias("first_known_next_requester_id"),
        F.min("requester_id_rank").over(w_distinct_agent_id_in_agent_t).alias("distinct_agent_id_in_agent_team_agent_tier_agent"),
        F.lag("second_csat").over(w_all_by_next_csat).alias("prev_second_csat"),
        F.size(F.collect_set("agent_id").over(w_distinct_agent_id_in_agent_t)).alias("distinct_agent_id_in_distinct_agent_id_in_agent_"),
        F.first("requester_id_max_3").over(w_second_csat_by_requester_id_2).alias("first_requester_id_max_3"),
        F.round(F.cume_dist().over(w_all_by_next_csat_ties), 3).alias("next_csat_cume_dist"),
    )
)

totals = (
    metrics
    .groupBy(
        "distinct_agent_id_in_agent_team_agent_tier",
        "second_csat",
        "distinct_channel_in_agent_id",
    )
    .agg(
        F.array_sort(F.collect_set("agent_name")).alias("agent_name_list"),
        F.round(F.sum("distinct_channel_in_agent_team_agent_tier") / F.nullif(F.sum("requester_id_rank"), F.lit(0)), 3).alias("distinct_channel_in_agent_team_agent_tier_to_req"),
        F.array_sort(F.collect_set("agent_team")).alias("agent_team_list"),
        F.count("*").alias("row_count"),
        F.round(F.sum("first_requester_id") / F.nullif(F.sum("requester_id_rank"), F.lit(0)), 3).alias("first_requester_id_to_requester_id_rank_ratio"),
        F.count(F.when(F.col("agent_team") == "Tech", 1)).alias("n_agent_team_tech"),
    )
    .filter(F.col("row_count") >= 3)
)

result = (
    totals
    .filter(F.size(F.col("agent_name_list")) >= 1)
    .select(
        "distinct_agent_id_in_agent_team_agent_tier",
        "second_csat",
        "distinct_channel_in_agent_id",
        "agent_name_list",
        "agent_team_list",
        "row_count",
        "first_requester_id_to_requester_id_rank_ratio",
        "n_agent_team_tech",
        F.when(F.col("row_count") >= 8, "HIGH").when(F.col("row_count") >= 5, "MEDIUM").otherwise("LOW").alias("row_count_tier"),
    )
)
