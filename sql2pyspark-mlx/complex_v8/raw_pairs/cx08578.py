from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_pct_tier_is_not_null = Window.orderBy(F.col("pct_tier_is_not_null").desc(), "hired_on_year", "team")
w_all_by_pct_tier_is_not_null_2 = Window.orderBy(F.col("pct_tier_is_not_null").desc(), "hired_on_year", "team").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_next2_n_length_trim_agent_na = Window.partitionBy("next2_n_length_trim_agent_name_4", "team")
w_next2_n_length_trim_agent_na_2 = Window.partitionBy("next2_n_length_trim_agent_name_4").orderBy(F.col("next2_n_length_trim_agent_name_4").desc(), "hired_on_year", "team").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_next2_n_length_trim_agent_na_3 = Window.partitionBy("next2_n_length_trim_agent_name_4").orderBy(F.col("next2_n_length_trim_agent_name_4").desc(), "hired_on_year", "team")

valid_agents = (
    df_support_agents
    .filter(F.col("tier").isin("T1", "T2"))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.date_trunc("week", F.col("hired_on")).alias("hired_on_week"),
        F.coalesce(F.col("tier"), F.lit("UNKNOWN")).alias("tier_or_unknown"),
    )
)

grouped = (
    valid_agents
    .groupBy(F.year("hired_on").alias("hired_on_year"), "team")
    .agg(
        F.countDistinct("tier").alias("distinct_tiers"),
        F.round(100 * F.count(F.when(F.col("tier").isNotNull(), 1)) / F.count("*"), 1).alias("pct_tier_is_not_null"),
        F.count(F.when(F.length(F.trim(F.col("agent_name"))) >= 4, 1)).alias("n_length_trim_agent_name_4"),
    )
)

windowed = (
    grouped
    .select(
        "hired_on_year",
        "team",
        "pct_tier_is_not_null",
        F.lead("n_length_trim_agent_name_4", 2).over(w_all_by_pct_tier_is_not_null).alias("next2_n_length_trim_agent_name_4"),
        F.ntile(5).over(w_all_by_pct_tier_is_not_null).alias("pct_tier_is_not_null_quintile"),
        F.rank().over(w_all_by_pct_tier_is_not_null).alias("pct_tier_is_not_null_rank"),
        F.sum("pct_tier_is_not_null").over(w_all_by_pct_tier_is_not_null_2).alias("running_pct_tier_is_not_null"),
    )
)

with_trends = (
    windowed
    .select(
        "hired_on_year",
        "team",
        "pct_tier_is_not_null_quintile",
        "running_pct_tier_is_not_null",
        F.round(F.col("running_pct_tier_is_not_null") - F.avg("running_pct_tier_is_not_null").over(w_next2_n_length_trim_agent_na), 2).alias("running_pct_tier_is_not_null_vs_next2_n_length_t"),
        F.last("pct_tier_is_not_null_rank").over(w_next2_n_length_trim_agent_na_2).alias("last_pct_tier_is_not_null_rank"),
        F.round(F.col("pct_tier_is_not_null_rank") - F.avg("pct_tier_is_not_null_rank").over(w_next2_n_length_trim_agent_na), 2).alias("pct_tier_is_not_null_rank_vs_next2_n_length_trim"),
        F.round(F.col("hired_on_year") / F.nullif(F.sum("hired_on_year").over(w_next2_n_length_trim_agent_na), F.lit(0)), 4).alias("hired_on_year_share_of_next2_n_length_trim_agent"),
        (F.col("pct_tier_is_not_null") - F.lag("pct_tier_is_not_null").over(w_all_by_pct_tier_is_not_null)).alias("pct_tier_is_not_null_change"),
        F.dense_rank().over(w_next2_n_length_trim_agent_na_3).alias("next2_n_length_trim_agent_name_4_dense_rank"),
    )
)

result = (
    with_trends
    .select(
        "hired_on_year",
        "team",
        F.col("pct_tier_is_not_null_quintile").alias("m_pct_tier_is_not_null_quintile"),
        F.col("last_pct_tier_is_not_null_rank").alias("last_pct_tier_is_not_null_rank_v"),
    )
    .unpivot(
        ids=["hired_on_year", "team"],
        values=["m_pct_tier_is_not_null_quintile", "last_pct_tier_is_not_null_rank_v"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
