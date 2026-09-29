from pyspark.sql import functions as F

claim_side = (
    df_claims
    .select(
        F.col("policy_id").alias("key_id"),
        F.col("id").alias("entity_id"),
        "claimed_amount",
        F.lit("claim").alias("origin"),
    )
)

policy_rows = (
    df_policies
    .filter(~F.col("status").isin("EXPIRED", "ACTIVE"))
    .select(
        F.col("holder_id").alias("key_id"),
        F.col("id").alias("entity_id"),
        F.col("premium").alias("claimed_amount"),
        F.lit("policy").alias("origin"),
    )
)

claim_rows = (
    df_claims
    .select(
        F.col("adjuster_id").alias("key_id"),
        F.col("policy_id").alias("entity_id"),
        "claimed_amount",
        F.lit("claim_3").alias("origin"),
    )
)

all_events = (
    claim_side
    .unionByName(policy_rows)
    .unionByName(claim_rows)
)

result = (
    all_events
    .groupBy("origin")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("claimed_amount").alias("total_claimed_amount"),
    )
    .orderBy("origin")
)
