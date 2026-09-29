from pyspark.sql import functions as F

prepared = (
    df_claims
    .filter(F.col("adjuster_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        F.round(F.col("claimed_amount") * 0.9, 2).alias("claimed_amount_adj"),
    )
)

claim_side = (
    df_claims
    .filter((F.col("claimed_amount") <= 42447.2) & (F.col("status") != "REJECTED"))
    .select("adjuster_id", "reported_at", "claim_type")
)

claim_side_2 = (
    df_claims
    .filter((F.col("claim_date") >= F.lit("2024-04-15").cast("date")) & (F.col("claimed_amount").between(11235.6, 42838.9)))
    .select("id", "reported_at", "status")
)

claim_side_3 = (
    df_claims
    .filter(F.col("claimed_amount") > 10518.4)
    .select("policy_id", "reported_at", "status")
)

result = (
    claim_side
    .union(claim_side_2)
    .union(claim_side_3)
    .orderBy(F.col("adjuster_id").desc(), "reported_at")
)
