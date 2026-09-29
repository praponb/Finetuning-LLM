from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_n_tier_t2_moving_2 = Window.orderBy("n_tier_t2", "hired_on_year", "tier").rowsBetween(-2, Window.currentRow)
w_n_upper_trim_agent_name_like = Window.partitionBy("n_upper_trim_agent_name_like_l", "n_tier_t2").orderBy("row_count")
w_n_upper_trim_agent_name_like_2 = Window.partitionBy("n_upper_trim_agent_name_like_l", "n_tier_t2").orderBy("row_count", "hired_on_year", "tier")
w_tier = Window.partitionBy("tier")
w_all_by_n_tier_t2_full = Window.orderBy("n_tier_t2", "hired_on_year", "tier").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_last_row_count = Window.partitionBy("last_row_count")
w_all_by_last_row_count_full = Window.orderBy(F.col("last_row_count").desc_nulls_first(), "hired_on_year", "tier").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_last_row_count_ties = Window.orderBy(F.col("last_row_count").desc_nulls_first())
w_all_by_last_row_count_prior = Window.orderBy(F.col("last_row_count").desc_nulls_first(), "hired_on_year", "tier").rowsBetween(-6, -1)
w_all_by_last_row_count = Window.orderBy(F.col("last_row_count").desc_nulls_first(), "hired_on_year", "tier")

filtered_agents = (
    df_support_agents
    .filter(F.upper(F.trim(F.col("agent_name"))).like("F%"))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
    )
)

totals = (
    filtered_agents
    .groupBy(F.year("hired_on").alias("hired_on_year"), "tier")
    .agg(
        F.sum(F.when(F.col("tier") == "T2", 1).otherwise(0)).alias("n_tier_t2"),
        F.array_sort(F.collect_set("agent_name")).alias("agent_name_list"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.upper(F.trim(F.col("agent_name"))).like("L%"), 1).otherwise(0)).alias("n_upper_trim_agent_name_like_l"),
    )
)

ranked = (
    totals
    .select(
        "hired_on_year",
        "tier",
        "n_tier_t2",
        "agent_name_list",
        F.max("row_count").over(w_all_by_n_tier_t2_moving_2).alias("row_count_max_3"),
        F.round(F.cume_dist().over(w_n_upper_trim_agent_name_like), 3).alias("row_count_cume_dist"),
        (F.col("row_count") - F.lag("row_count").over(w_n_upper_trim_agent_name_like_2)).alias("row_count_change"),
        F.max("row_count").over(w_tier).alias("tier_max_row_count"),
        F.last("row_count").over(w_all_by_n_tier_t2_full).alias("last_row_count"),
        F.sum("hired_on_year").over(w_tier).alias("tier_sum_hired_on_year"),
    )
)

with_activity = (
    ranked
    .crossJoin(ranked.agg(F.max("tier_sum_hired_on_year").alias("overall_max_tier_sum_hired_on_year")))
    .filter(F.col("tier_sum_hired_on_year") < F.col("overall_max_tier_sum_hired_on_year"))
    .drop("overall_max_tier_sum_hired_on_year")
)

ranked_2 = (
    with_activity
    .select(
        "*",
        F.round(F.col("last_row_count") / F.nullif(F.sum("last_row_count").over(w_last_row_count), F.lit(0)), 4).alias("last_row_count_share_of_last_row_count"),
        F.first("last_row_count").over(w_all_by_last_row_count_full).alias("first_last_row_count"),
        F.round(F.cume_dist().over(w_all_by_last_row_count_ties), 3).alias("last_row_count_cume_dist"),
        F.round(F.avg("row_count_max_3").over(w_all_by_last_row_count_prior), 2).alias("row_count_max_3_avg_6_prior"),
        F.lead("row_count_cume_dist").over(w_all_by_last_row_count).alias("next_row_count_cume_dist"),
    )
)

without_matches = (
    ranked_2
    .crossJoin(ranked_2.agg(F.max("n_tier_t2").alias("overall_max_n_tier_t2")))
    .filter(F.col("n_tier_t2") < F.col("overall_max_n_tier_t2"))
    .drop("overall_max_n_tier_t2")
)

result = (
    without_matches
    .select(
        "hired_on_year",
        "tier",
        "n_tier_t2",
        "agent_name_list",
        "row_count_max_3",
        "row_count_cume_dist",
        "row_count_change",
        "tier_max_row_count",
        "last_row_count",
        "tier_sum_hired_on_year",
        "first_last_row_count",
        "last_row_count_cume_dist",
        "row_count_max_3_avg_6_prior",
        "next_row_count_cume_dist",
        F.abs(F.col("last_row_count_cume_dist") - 0.5).alias("last_row_count_cume_dist_distance"),
        (F.col("row_count_change") + F.col("next_row_count_cume_dist")).alias("row_count_change_plus_next_row_count_cume_dist"),
    )
    .orderBy("tier", "hired_on_year")
    .limit(10)
)
