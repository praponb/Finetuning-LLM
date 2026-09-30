from pyspark.sql import functions as F

scoped_claims = (
    df_claims
    .filter(F.col("claim_date").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.concat(F.col("status"), F.lit("#"), F.col("policy_id").cast("string")).alias("status_ref"),
        F.when(F.col("status").isin("OPEN", "PAID"), "group_a").when(F.col("status") == "IN_REVIEW", "group_b").otherwise("other").alias("status_group"),
        F.when(F.col("approved_amount") >= 15857.9, "A").when(F.col("approved_amount") >= 492.6, "B").otherwise("C").alias("approved_amount_tier"),
    )
)

stats = (
    scoped_claims
    .groupBy(F.to_date("reported_at").alias("reported_at_day"))
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("status_group").alias("distinct_status_groups"),
        F.round(F.sum("approved_amount"), 2).alias("total_approved_amount"),
        F.sum(F.when(~F.col("claim_type").isin("medical", "delay"), 1).otherwise(0)).alias("n_claim_type_not_in_medical_de"),
        F.round(F.sum("approved_amount") / F.nullif(F.sum("claimed_amount"), F.lit(0)), 3).alias("approved_amount_to_claimed_amount_ratio"),
        F.count(F.when(F.col("claim_type") == "collision", 1)).alias("n_claim_type_collision"),
    )
)

qualified = (
    stats
    .crossJoin(stats.agg(F.avg("n_claim_type_not_in_medical_de").alias("overall_avg_n_claim_type_not_in_medical_de")))
    .filter(F.col("n_claim_type_not_in_medical_de") > F.col("overall_avg_n_claim_type_not_in_medical_de"))
    .drop("overall_avg_n_claim_type_not_in_medical_de")
)

result = (
    qualified
    .select(
        "reported_at_day",
        F.col("approved_amount_to_claimed_amount_ratio").alias("m_approved_amount_to_claimed_amount_ratio"),
        F.col("total_approved_amount").alias("total_approved_amount_v"),
    )
    .unpivot(
        ids=["reported_at_day"],
        values=["m_approved_amount_to_claimed_amount_ratio", "total_approved_amount_v"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
