from pyspark.sql import functions as F

prepared = (
    df_policies
    .filter((F.col("status") == "CANCELLED") & (F.col("premium") <= 3544.1))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
    )
)

with_children = (
    prepared.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.status").alias("claim_status"),
    )
)

with_children_claims = (
    with_children.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.claim_id",
        "s.claim_approved_amount",
        "s.claim_status",
        F.col("c.id").alias("claim_id_2"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.approved_amount").alias("claim_approved_amount_2"),
    )
)

enriched = (
    with_children_claims
    .filter(F.col("claim_approved_amount_2") > 0.0)
    .withColumn("product_claim_status_label", F.concat_ws(" / ", F.col("product"), F.col("claim_status")))
)

result = (
    enriched
    .select(
        "id",
        "claim_id",
        "claim_approved_amount",
        "claim_approved_amount_2",
    )
    .unpivot(
        ids=["id", "claim_id"],
        values=["claim_approved_amount", "claim_approved_amount_2"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .orderBy("id", "claim_id", "field")
)
