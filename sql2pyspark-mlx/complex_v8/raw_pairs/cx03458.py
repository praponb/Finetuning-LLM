from pyspark.sql import functions as F

base = (
    df_claims
    .filter((F.month(F.col("reported_at")).isin(2, 3)) & (F.col("status").isin("PAID", "REJECTED", "APPROVED")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
    )
)

claim_side = (
    df_claims
    .filter((F.col("approved_amount") <= 15477.7) & (F.col("status").isin("PAID", "REJECTED")))
    .select(
        F.col("id").alias("qty"),
        F.col("claim_date").alias("event_date"),
        F.col("policy_id").alias("entity_id"),
    )
)

policy_side = (
    df_policies
    .filter((F.col("start_date").isNotNull()) & (F.col("premium") < 3465.4))
    .select("id", "start_date", "holder_id")
)

policy_rows = df_policies.select("id", "end_date", "holder_id")

combined = (
    claim_side
    .union(policy_side)
    .union(policy_rows)
)

result = (
    combined
    .groupBy("qty")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("entity_id").alias("total_entity_id"),
        F.max("event_date").alias("latest_event_date"),
    )
    .orderBy("qty")
)
