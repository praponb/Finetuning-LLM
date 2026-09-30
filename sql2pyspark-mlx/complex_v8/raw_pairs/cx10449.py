from pyspark.sql import functions as F

filtered_claims = (
    df_claims
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        F.date_format(F.col("claim_date"), "yyyy-MM-dd").alias("claim_date_label"),
    )
)

filtered = (
    filtered_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.status").isin("OPEN", "REJECTED")), "left_anti")
    .filter(F.col("claim_date") >= F.lit("2024-04-15").cast("date"))
)

per_group = (
    filtered
    .groupBy(F.year("claim_date").alias("claim_date_year"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("claim_type") == "theft", F.col("claimed_amount"))).alias("claimed_amount_claim_type_theft"),
        F.datediff(F.max("claim_date"), F.min("claim_date")).alias("claim_date_span_days"),
        F.min("claim_date").alias("first_claim_date"),
        F.sum(F.when(F.col("claim_type") != "flood", 1).otherwise(0)).alias("n_claim_type_flood"),
    )
)

result = (
    per_group
    .filter(F.col("distinct_ids") <= 8)
    .select(
        "claim_date_year",
        "claimed_amount_claim_type_theft",
        "claim_date_span_days",
        "first_claim_date",
        "n_claim_type_flood",
    )
    .orderBy(F.col("claim_date_year").desc(), "n_claim_type_flood")
    .limit(5)
)
