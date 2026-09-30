from pyspark.sql import functions as F

prepared = (
    df_claims
    .filter((F.col("status") == "IN_REVIEW") & (F.col("adjuster_id") <= 3))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.date_add(F.col("claim_date"), 365).alias("claim_date_plus_365d"),
        F.when(F.col("claim_type").isin("fire", "delay"), "group_a").when(F.col("claim_type") == "medical", "group_b").otherwise("other").alias("claim_type_group"),
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
    )
)

with_details = (
    prepared.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.claim_date_plus_365d",
        "s.claim_type_group",
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
        F.col("p.product").alias("policy_product"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

result = (
    with_details
    .select("id", "claimed_amount", "approved_amount")
    .unpivot(
        ids=["id"],
        values=["claimed_amount", "approved_amount"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "measure")
)
