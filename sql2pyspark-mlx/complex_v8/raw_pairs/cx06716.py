from pyspark.sql import functions as F

prepared = (
    df_claims
    .filter((F.col("claimed_amount") >= 5774.6) & (F.col("approved_amount").between(0.0, 26237.1)))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
    )
)

with_details = (
    prepared.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.*",
        F.col("p.status").alias("policy_status"),
        F.col("p.holder_id").alias("policy_holder_id"),
    )
)

policy_holder_id_stats = (
    with_details
    .groupBy("policy_holder_id")
    .agg(F.max("claimed_amount").alias("max_claimed_amount"))
)

above_average = (
    with_details.alias("s")
    .join(policy_holder_id_stats.alias("g"), F.col("g.policy_holder_id") == F.col("s.policy_holder_id"), "inner")
    .filter(F.col("s.claimed_amount") == F.col("g.max_claimed_amount"))
    .select(
        "s.*",
        "g.max_claimed_amount",
        F.round(F.col("s.claimed_amount") - F.col("g.max_claimed_amount"), 2).alias("claimed_amount_minus_max"),
    )
)

result = (
    above_average
    .select("id", "claimed_amount", "claimed_amount_minus_max")
    .unpivot(
        ids=["id"],
        values=["claimed_amount", "claimed_amount_minus_max"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "field")
)
