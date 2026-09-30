from pyspark.sql import functions as F

filtered = (
    df_claims
    .filter((F.col("reported_at") >= F.lit("2025-02-12 07:00:00").cast("timestamp")) | (~F.col("status").isin("APPROVED", "PAID")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "reported_at",
    )
)

claim_rows = filtered.select("id", "adjuster_id", "claimed_amount")

policy_rows = df_policies.select("holder_id", "id", "premium")

policy_side = (
    df_policies
    .filter(F.col("start_date") < F.lit("2025-05-30").cast("date"))
    .select("holder_id", "id", "premium")
)

combined = (
    claim_rows
    .union(policy_rows)
    .union(policy_side)
)

result = (
    combined
    .groupBy("id")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("claimed_amount").alias("total_claimed_amount"),
    )
    .orderBy("id")
)
