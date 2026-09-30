from pyspark.sql import functions as F

filtered_claims = (
    df_claims
    .filter((F.hour(F.col("reported_at")).between(7, 13)) & (F.col("claim_type") == "fire"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        F.round(F.col("approved_amount") * 1.1, 2).alias("approved_amount_adj"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
    )
)

totals = (
    filtered_claims
    .filter(F.col("days_since_reported_at").between(140, 560))
    .groupBy("claim_type")
    .agg(F.countDistinct("policy_id").alias("distinct_policy_ids"))
)

filtered = (
    totals
    .crossJoin(totals.agg(F.max("distinct_policy_ids").alias("overall_max_distinct_policy_ids")))
    .filter(F.col("distinct_policy_ids") < F.col("overall_max_distinct_policy_ids"))
    .drop("overall_max_distinct_policy_ids")
)

result = (
    filtered
    .filter(F.col("distinct_policy_ids") > 3)
    .select(
        "claim_type",
        "distinct_policy_ids",
        (F.floor(F.col("distinct_policy_ids") / 2) * 2).alias("distinct_policy_ids_bucket"),
    )
    .orderBy(F.col("distinct_policy_ids_bucket").desc(), "claim_type")
    .limit(5)
)
