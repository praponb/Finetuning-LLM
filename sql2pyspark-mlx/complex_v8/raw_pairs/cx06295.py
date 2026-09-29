from pyspark.sql import functions as F

cleaned_holders = (
    df_policyholders
    .filter((F.col("joined_on") >= F.lit("2024-10-23").cast("date")) & (F.col("segment").isin("VIP", "SME")))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "segment",
        F.lower(F.trim(F.col("segment"))).alias("segment_clean"),
    )
)

summary = (
    cleaned_holders
    .filter(F.col("segment") == "VIP")
    .groupBy("region", "segment")
    .agg(
        F.count("*").alias("cnt"),
        F.sum("risk_score").alias("total_risk_score"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("total_risk_score") > 0.2)
)

labeled = (
    summary
    .filter(F.col("distinct_ids") >= 2)
    .select(
        "*",
        F.when(F.col("segment").isin("VIP", "SME"), "group_a").when(F.col("segment") == "RETAIL", "group_b").otherwise("other").alias("segment_group"),
        F.round(F.col("total_risk_score") * 0.9, 2).alias("total_risk_score_adj"),
    )
)

qualified = (
    labeled
    .crossJoin(labeled.agg(F.avg("distinct_ids").alias("overall_avg_distinct_ids")))
    .filter(F.col("distinct_ids") > F.col("overall_avg_distinct_ids"))
    .drop("overall_avg_distinct_ids")
)

qualified_2 = (
    qualified
    .crossJoin(qualified.agg(F.min("total_risk_score_adj").alias("overall_min_total_risk_score_adj")))
    .filter(F.col("total_risk_score_adj") > F.col("overall_min_total_risk_score_adj"))
    .drop("overall_min_total_risk_score_adj")
)

result = (
    qualified_2
    .select(
        "region",
        "segment",
        "segment_group",
        F.concat(F.col("segment"), F.lit("#"), F.col("cnt").cast("string")).alias("segment_ref"),
    )
    .orderBy(F.col("segment_ref").desc(), "region", "segment")
    .limit(20)
)
