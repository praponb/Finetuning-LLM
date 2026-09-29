from pyspark.sql import functions as F

matched = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "left",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.adjuster_id",
        "c.approved_amount",
        "p.product",
        "p.premium",
    )
)

summary = (
    matched
    .groupBy(
        "policy_id",
        F.date_trunc("month", "claim_date").alias("claim_date_month"),
    )
    .agg(
        F.countDistinct("adjuster_id").alias("distinct_adjuster_ids"),
        F.count("*").alias("n_rows"),
        F.count(F.when(F.col("product").isNotNull(), 1)).alias("n_product_is_not_null"),
    )
)

tagged = (
    summary
    .filter(F.col("n_product_is_not_null") > 2)
    .select(
        "*",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("claim_date_month")).alias("days_since_claim_date_month"),
        (F.floor(F.col("n_product_is_not_null") / 2) * 2).alias("n_product_is_not_null_bucket"),
        F.date_format(F.col("claim_date_month"), "MMM yyyy").alias("claim_date_month_label"),
    )
)

result = (
    tagged
    .select(
        "policy_id",
        "claim_date_month",
        F.col("distinct_adjuster_ids").alias("distinct_adjuster_ids_v"),
        F.col("n_rows").alias("m_n_rows"),
        F.col("n_product_is_not_null").alias("m_n_product_is_not_null"),
    )
    .unpivot(
        ids=["policy_id", "claim_date_month"],
        values=["distinct_adjuster_ids_v", "m_n_rows", "m_n_product_is_not_null"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
