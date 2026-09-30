from pyspark.sql import functions as F

filtered_policys = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) & (F.col("status").isin("CANCELLED", "LAPSED", "ACTIVE")))
)

combined_details = (
    filtered_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.status",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.region").alias("holder_region"),
        F.col("p.segment").alias("holder_segment"),
    )
)

exploded_detail_claims = (
    combined_details.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.holder_joined_on",
        "s.holder_region",
        F.col("c.id").alias("claim_id"),
        "c.claim_date",
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.status").alias("claim_status"),
    )
)

result = (
    exploded_detail_claims
    .select(
        "id",
        "claim_id",
        "claim_claimed_amount",
        "premium",
    )
    .unpivot(
        ids=["id", "claim_id"],
        values=["claim_claimed_amount", "premium"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
