from pyspark.sql import functions as F

filtered = (
    df_claims
    .filter((F.col("policy_id") <= 3) & (F.col("claim_type").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "reported_at",
        F.concat_ws(": ", F.col("claim_type"), F.col("status")).alias("claim_type_status_label"),
        F.when(F.col("status").isin("PAID", "REJECTED"), "group_a").when(F.col("status") == "IN_REVIEW", "group_b").otherwise("other").alias("status_group"),
        F.date_trunc("hour", F.col("reported_at")).alias("reported_at_hour"),
    )
)

scored = (
    filtered
    .filter(F.col("reported_at_hour") < F.lit("2025-02-05 09:00:00").cast("timestamp"))
    .select(
        "id",
        "adjuster_id",
        "approved_amount",
        "reported_at",
        "claim_type_status_label",
        "status_group",
        F.substring(F.trim(F.col("claim_type")), 1, 4).alias("claim_type_prefix"),
    )
)

result = (
    scored
    .groupBy("adjuster_id")
    .pivot("status_group", ["group_a", "group_b", "other"])
    .agg(F.sum("approved_amount"))
    .orderBy("adjuster_id")
)
