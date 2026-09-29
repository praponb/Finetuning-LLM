from pyspark.sql import functions as F

valid = (
    df_claims
    .filter((F.col("claim_date").isNotNull()) | (F.col("status").isin("PAID", "OPEN")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.least(F.col("approved_amount"), F.col("claimed_amount")).alias("least_approved_amount_claimed_amount"),
        (F.col("claimed_amount") + F.col("approved_amount")).alias("claimed_amount_plus_approved_amount"),
    )
)

scored = (
    valid
    .filter(F.col("claim_date") < F.lit("2025-05-26").cast("date"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "least_approved_amount_claimed_amount",
        F.when(F.col("claim_type").isin("fire", "delay"), "group_a").when(F.col("claim_type") == "medical", "group_b").otherwise("other").alias("claim_type_group"),
    )
)

grouped = (
    scored
    .groupBy("policy_id")
    .agg(
        F.count(F.when(F.col("claim_type") != "theft", 1)).alias("n_claim_type_theft"),
        F.count("*").alias("row_count"),
        F.datediff(F.max("claim_date"), F.min("claim_date")).alias("claim_date_span_days"),
        F.round(F.sum("approved_amount") / F.nullif(F.sum("least_approved_amount_claimed_amount"), F.lit(0)), 3).alias("approved_amount_to_least_approved_amount_claimed"),
    )
)

result = (
    grouped
    .select(
        "policy_id",
        F.col("n_claim_type_theft").alias("n_claim_type_theft_v"),
        F.col("row_count").alias("row_count_v"),
    )
    .unpivot(
        ids=["policy_id"],
        values=["n_claim_type_theft_v", "row_count_v"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
