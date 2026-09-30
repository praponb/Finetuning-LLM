from pyspark.sql import functions as F

filtered_claims = (
    df_claims
    .filter(F.col("adjuster_id") >= 6)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.when(F.col("claim_type").isin("fire", "collision"), "group_a").when(F.col("claim_type") == "medical", "group_b").otherwise("other").alias("claim_type_group"),
    )
)

claim_side = (
    filtered_claims
    .filter((F.col("adjuster_id").isNotNull()) & (F.col("status").isNotNull()))
    .select(
        F.lit("claim").alias("src"),
        F.col("policy_id").alias("entity_id"),
        "claim_type_group",
        "claim_date",
    )
)

claim_rows = (
    df_claims
    .select(
        F.lit("claim_2").alias("src"),
        F.col("id").alias("entity_id"),
        F.col("status").alias("claim_type_group"),
        "claim_date",
    )
)

policy_rows = (
    df_policies
    .filter(F.col("premium") > 2111.3)
    .select(
        F.lit("policy").alias("src"),
        F.col("id").alias("entity_id"),
        F.col("status").alias("claim_type_group"),
        F.col("end_date").alias("claim_date"),
    )
)

result = (
    claim_side
    .union(claim_rows)
    .union(policy_rows)
    .orderBy(F.col("src").desc(), "claim_type_group")
)
