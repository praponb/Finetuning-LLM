from pyspark.sql import functions as F

filtered = (
    df_claims
    .filter(F.col("policy_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.round(F.col("approved_amount") * 1.5, 2).alias("approved_amount_adj"),
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
        F.abs(F.col("claimed_amount") - 29985.9).alias("claimed_amount_distance"),
    )
)

derived = (
    filtered
    .select(
        "*",
        F.when(F.col("status").isin("IN_REVIEW", "REJECTED"), "group_a").when(F.col("status") == "APPROVED", "group_b").otherwise("other").alias("status_group"),
        (F.floor(F.col("approved_amount_adj") / 10000) * 10000).alias("approved_amount_adj_bucket"),
        F.concat(F.col("claim_type"), F.lit("#"), F.col("claim_type_len").cast("string")).alias("claim_type_ref"),
    )
)

result = (
    derived
    .cube("claim_type_len", "status", "policy_id")
    .agg(
        F.round(F.sum("approved_amount_adj_bucket") / F.nullif(F.sum("approved_amount"), F.lit(0)), 3).alias("approved_amount_adj_bucket_to_approved_amount_ra"),
        F.max("approved_amount_adj_bucket").alias("max_approved_amount_adj_bucket"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
    )
    .select(
        "claim_type_len",
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "policy_id",
        "approved_amount_adj_bucket_to_approved_amount_ra",
        "max_approved_amount_adj_bucket",
        "status_list",
    )
)
