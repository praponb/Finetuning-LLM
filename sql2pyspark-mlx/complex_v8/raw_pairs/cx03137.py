from pyspark.sql import functions as F

filtered = (
    df_policies
    .filter((~F.col("product").isin("HEALTH", "TRAVEL")) & (F.col("status") == "CANCELLED"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.abs(F.col("premium") - 3521.8).alias("premium_distance"),
    )
)

combined_details_holder = (
    filtered.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.end_date",
        "s.status",
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

screened = (
    combined_details_holder.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.product") == "LIFE"), "left_anti")
)

with_children_claims = (
    screened.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.status",
        "s.holder_joined_on",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        "c.claim_date",
    )
)

result = (
    with_children_claims
    .filter(F.year(F.col("holder_joined_on")) == 2025)
    .select(
        "id",
        "status",
        "holder_joined_on",
        "claim_id",
        "claim_adjuster_id",
        "claim_approved_amount",
    )
    .orderBy(F.col("status").desc())
)
