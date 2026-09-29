from pyspark.sql import functions as F

cleaned_holders = (
    df_policyholders
    .filter((F.col("segment") != "VIP") & (F.length(F.trim(F.col("holder_name"))) >= 4))
    .select(
        "id",
        "risk_score",
        "joined_on",
        "segment",
        F.when(F.col("risk_score") >= 0.6, "large").when(F.col("risk_score") >= 0.2, "medium").otherwise("small").alias("risk_score_tier"),
        F.when(F.col("risk_score") >= 0.8, "HIGH").when(F.col("risk_score") >= 0.2, "MEDIUM").otherwise("LOW").alias("risk_score_tier_2"),
        F.round(F.col("risk_score") * 1.1, 2).alias("risk_score_adj"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.max("start_date").alias("last_policy_start_date"),
        F.max("end_date").alias("last_policy_end_date"),
    )
)

with_counts = (
    cleaned_holders.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left")
    .select("s.*", "p.last_policy_start_date", "p.last_policy_end_date")
)

enriched = (
    with_counts
    .withColumn("risk_score_tier_or_unknown", F.coalesce(F.col("risk_score_tier"), F.lit("UNKNOWN")))
)

qualified = (
    enriched
    .crossJoin(enriched.agg(F.avg("risk_score_adj").alias("overall_avg_risk_score_adj")))
    .filter(F.col("risk_score_adj") > F.col("overall_avg_risk_score_adj"))
    .drop("overall_avg_risk_score_adj")
)

result = (
    qualified
    .select(
        "id",
        "joined_on",
        "segment",
        "risk_score_tier_2",
        "risk_score_adj",
        "last_policy_start_date",
        "last_policy_end_date",
        F.when(F.col("risk_score_adj") >= 1.2, "HIGH").when(F.col("risk_score_adj") >= 0.6, "MEDIUM").otherwise("LOW").alias("risk_score_adj_tier"),
        F.when(F.col("risk_score") >= 0.7, "large").when(F.col("risk_score") >= 0.2, "medium").otherwise("small").alias("risk_score_tier_3"),
    )
    .orderBy(F.col("last_policy_end_date").desc(), "id")
    .limit(10)
)
