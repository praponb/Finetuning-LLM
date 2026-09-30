from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_region_by_joined_on_running = Window.partitionBy("region").orderBy("joined_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_region_by_joined_on_full = Window.partitionBy("region").orderBy("joined_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_name_len_policy_count = Window.partitionBy("holder_name_len", "policy_count")
w_region_by_joined_on_ties = Window.partitionBy("region").orderBy("joined_on")
w_region_by_joined_on = Window.partitionBy("region").orderBy("joined_on", "id")

cleaned_holders = (
    df_policyholders
    .filter(F.col("region").isin("Central", "South", "North"))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
        F.length(F.trim(F.col("holder_name"))).alias("holder_name_len"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.count("*").alias("policy_count"),
        F.min("start_date").alias("first_policy_start_date"),
        F.max("end_date").alias("last_policy_end_date"),
    )
)

profiled = (
    cleaned_holders.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
        "p.first_policy_start_date",
        "p.last_policy_end_date",
    )
)

windowed = (
    profiled
    .select(
        "*",
        F.min("risk_score").over(w_region_by_joined_on_running).alias("min_risk_score_so_far"),
        F.nth_value("joined_on", 2).over(w_region_by_joined_on_full).alias("second_joined_on"),
        F.size(F.collect_set("region").over(w_holder_name_len_policy_count)).alias("distinct_region_in_holder_name_len_policy_count"),
        F.rank().over(w_region_by_joined_on_ties).alias("joined_on_rank"),
        F.sum("policy_count").over(w_holder_name_len_policy_count).alias("holder_name_len_policy_count_sum_policy_count"),
        F.round(F.percent_rank().over(w_region_by_joined_on), 3).alias("joined_on_percent_rank"),
    )
)

result = (
    windowed
    .select(
        "id",
        F.col("min_risk_score_so_far").alias("m_min_risk_score_so_far"),
        F.col("joined_on_percent_rank").alias("joined_on_percent_rank_v"),
        F.col("risk_score").alias("m_risk_score"),
    )
    .unpivot(
        ids=["id"],
        values=["m_min_risk_score_so_far", "joined_on_percent_rank_v", "m_risk_score"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
