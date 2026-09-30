from pyspark.sql import functions as F

cleaned_claims = (
    df_claims
    .filter((F.col("status") != "IN_REVIEW") | (~F.col("claim_type").isin("collision", "medical")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        F.date_format(F.col("reported_at"), "MMM yyyy").alias("reported_at_label"),
        F.round(F.col("claimed_amount") * 1.1, 2).alias("claimed_amount_adj"),
    )
)

tagged = (
    cleaned_claims
    .select(
        "*",
        F.abs(F.col("claimed_amount") - 32441.2).alias("claimed_amount_distance"),
        F.initcap(F.trim(F.col("reported_at_label"))).alias("reported_at_label_clean"),
    )
)

result = (
    tagged
    .cube("claim_type", "status", "policy_id")
    .agg(
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.round(F.avg("claimed_amount"), 2).alias("avg_claimed_amount"),
    )
    .select(
        F.coalesce(F.col("claim_type"), F.lit("ALL")).alias("claim_type"),
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "policy_id",
        "status_list",
        "avg_claimed_amount",
    )
    .orderBy(
        F.col("claim_type").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
    )
)
