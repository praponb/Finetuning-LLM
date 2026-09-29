from pyspark.sql import functions as F

valid_holders = (
    df_policyholders
    .select(
        "id",
        "holder_name",
        "region",
        "joined_on",
        "segment",
        F.coalesce(F.col("risk_score"), F.lit(0.0)).alias("risk_score_filled"),
        F.coalesce(F.col("risk_score"), F.lit(0.0)).alias("risk_score_filled_2"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.min("end_date").alias("first_policy_end_date"),
        F.min("end_date").alias("first_policy_end_date_2"),
    )
)

summarized_rows = (
    valid_holders.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left")
    .select("s.*", "p.first_policy_end_date", "p.first_policy_end_date_2")
)

result = (
    summarized_rows
    .groupingSets(
        [["segment", "region"], ["segment"], []],
        "segment", "region",
    )
    .agg(
        F.round(F.sum("risk_score_filled"), 2).alias("total_risk_score_filled"),
        F.round(F.sum("risk_score_filled_2"), 2).alias("total_risk_score_filled_2"),
        F.array_sort(F.collect_set("region")).alias("region_list"),
    )
    .orderBy(F.col("segment").asc_nulls_last(), F.col("region").asc_nulls_last())
)
