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

derived = (
    matched
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "product",
        F.initcap(F.trim(F.col("product"))).alias("product_clean"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count(F.when(F.col("status").isNotNull(), 1)).alias("claim_status_count"),
        F.sum("approved_amount").alias("total_claim_approved_amount"),
        F.min("reported_at").alias("first_claim_reported_at"),
    )
)

with_counts = (
    derived.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
        "c.total_claim_approved_amount",
        "c.first_claim_reported_at",
    )
)

claim_stats_2 = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.count("*").alias("claim_count"), F.count("*").alias("claim_count_2"))
)

summarized_rows = (
    with_counts.alias("s")
    .join(claim_stats_2.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        F.coalesce(F.col("c.claim_count_2"), F.lit(0)).alias("claim_count_2"),
    )
)

result = (
    summarized_rows
    .select(
        "id",
        F.col("approved_amount").alias("m_approved_amount"),
        F.col("total_claim_approved_amount").alias("m_total_claim_approved_amount"),
    )
    .unpivot(
        ids=["id"],
        values=["m_approved_amount", "m_total_claim_approved_amount"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
