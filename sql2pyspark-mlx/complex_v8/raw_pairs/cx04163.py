from pyspark.sql import functions as F

recent_claims = (
    df_claims
    .filter(F.col("claim_type") != "delay")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "reported_at",
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
    )
)

claim_side = (
    recent_claims
    .filter((F.col("status_ref").isNotNull()) & (F.col("policy_id").isNotNull()))
    .select(F.lit("claim").alias("origin"), "id", "reported_at")
)

claim_side_2 = (
    df_claims
    .filter(F.col("claim_date") >= F.lit("2024-05-14").cast("date"))
    .select(
        F.lit("claim_2").alias("origin"),
        F.col("policy_id").alias("id"),
        "reported_at",
    )
)

combined = (
    claim_side
    .unionByName(claim_side_2)
    .distinct()
)

result = (
    combined
    .groupBy("origin")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("id").alias("total_id"),
        F.max("reported_at").alias("latest_reported_at"),
    )
    .orderBy("origin")
)
