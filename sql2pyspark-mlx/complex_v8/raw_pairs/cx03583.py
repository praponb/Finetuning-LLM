from pyspark.sql import functions as F

matched = (
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
        "c.approved_amount",
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

summary = (
    matched
    .groupBy("policy_id")
    .agg(
        F.round(100 * F.count(F.when(~F.col("product").isin("HEALTH", "HOME"), 1)) / F.count("*"), 1).alias("pct_product_not_in_health_home"),
        F.round(F.stddev("premium"), 2).alias("stddev_premium"),
        F.datediff(F.max("claim_date"), F.min("claim_date")).alias("claim_date_span_days"),
        F.round(F.sum("premium") / F.nullif(F.sum("approved_amount"), F.lit(0)), 3).alias("premium_to_approved_amount_ratio"),
        F.avg("claimed_amount").alias("avg_claimed_amount_check"),
    )
    .filter(F.col("avg_claimed_amount_check") > 6745.1)
    .drop("avg_claimed_amount_check")
)

with_parent = (
    summary.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .select(
        "s.*",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.status").alias("policy_status"),
        F.col("po.region").alias("holder_region"),
    )
)

result = (
    with_parent
    .select(
        "policy_id",
        "pct_product_not_in_health_home",
        "claim_date_span_days",
        "policy_status",
        "holder_region",
    )
    .orderBy("pct_product_not_in_health_home")
)
