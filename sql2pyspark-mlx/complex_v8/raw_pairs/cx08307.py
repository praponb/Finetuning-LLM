from pyspark.sql import functions as F

filtered_claims = (
    df_claims
    .filter((F.col("status").isin("OPEN", "PAID")) & (F.col("adjuster_id") >= 6))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
    )
)

stats = (
    filtered_claims
    .groupBy("claim_type")
    .agg(
        F.round(100 * F.count(F.when(F.col("claim_type") == "medical", 1)) / F.count("*"), 1).alias("pct_claim_type_medical"),
        F.count("*").alias("n_rows"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.min("approved_amount").alias("min_approved_amount"),
    )
)

annotated = (
    stats
    .filter(F.col("n_rows").between(3, 10))
    .select(
        "*",
        F.round(F.col("pct_claim_type_medical") * 1.1, 2).alias("pct_claim_type_medical_adj"),
        F.round(F.col("pct_claim_type_medical") * 0.9, 2).alias("pct_claim_type_medical_adj_2"),
    )
)

result = (
    annotated
    .select(
        "claim_type",
        "pct_claim_type_medical",
        "min_approved_amount",
        "pct_claim_type_medical_adj",
    )
    .unpivot(
        ids=["claim_type"],
        values=["pct_claim_type_medical", "min_approved_amount", "pct_claim_type_medical_adj"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
)
