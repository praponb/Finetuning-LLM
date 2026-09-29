from pyspark.sql import functions as F

covered = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "inner",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.adjuster_id",
        "c.claim_type",
        "p.product",
        "p.premium",
    )
)

exploded_detail_claims = (
    covered.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claim_type") == "medical"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.adjuster_id",
        "s.product",
        "s.premium",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.claim_type").alias("claim_claim_type"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

per_group = (
    exploded_detail_claims
    .filter(F.col("claim_status").isin("REJECTED", "OPEN", "APPROVED"))
    .groupBy(
        "product",
        F.date_trunc("month", "claim_date").alias("claim_date_month"),
        "claim_claim_type",
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("claim_status") != "REJECTED", 1)) / F.count("*"), 1).alias("pct_claim_status_rejected"),
        F.count("*").alias("row_count"),
        F.sum("premium").alias("total_premium"),
        F.count(F.when(F.col("claim_claim_type").isNotNull(), 1)).alias("n_claim_claim_type_is_not_null"),
        F.countDistinct("claim_claim_type").alias("distinct_claim_claim_types"),
    )
    .filter(F.col("row_count") >= 2)
)

result = (
    per_group
    .select(
        "product",
        "claim_date_month",
        "distinct_claim_claim_types",
        "n_claim_claim_type_is_not_null",
    )
    .unpivot(
        ids=["product", "claim_date_month"],
        values=["distinct_claim_claim_types", "n_claim_claim_type_is_not_null"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
