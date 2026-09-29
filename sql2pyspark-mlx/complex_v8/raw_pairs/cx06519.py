from pyspark.sql import functions as F

cleaned_holders = (
    df_policyholders
    .filter(F.length(F.trim(F.col("holder_name"))) >= 7)
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
        F.abs(F.col("risk_score") - 0.2).alias("risk_score_distance"),
        F.coalesce(F.col("region"), F.lit("UNKNOWN")).alias("region_or_unknown"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.sum("premium").alias("total_policy_premium"),
        F.sum("premium").alias("total_policy_premium_2"),
    )
)

summarized_rows = (
    cleaned_holders.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left")
    .select("s.*", "p.total_policy_premium", "p.total_policy_premium_2")
)

result = (
    summarized_rows
    .groupingSets(
        [["segment", "region", "region_or_unknown"], ["segment"], ["region"], []],
        "segment", "region", "region_or_unknown",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sum("total_policy_premium_2").alias("total_total_policy_premium_2"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(
        F.col("segment").asc_nulls_last(),
        F.col("region").asc_nulls_last(),
        F.col("region_or_unknown").asc_nulls_last(),
    )
)
