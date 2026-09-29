from pyspark.sql import functions as F

prepared_claims = (
    df_claims
    .filter(F.col("adjuster_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "reported_at",
        F.add_months(F.col("claim_date"), 1).alias("claim_date_plus_1m"),
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
    )
)

combined_details_policy = (
    prepared_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.col("p.product").alias("policy_product"),
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
    )
)

qualified = (
    combined_details_policy.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.approved_amount") < 14377.6) & (F.col("c.status") == "REJECTED"), "left_anti")
    .filter(F.upper(F.trim(F.col("status_clean"))).like("I%"))
)

result = (
    qualified
    .rollup("adjuster_id", "claim_type", "policy_product")
    .agg(
        F.count("*").alias("cnt"),
        F.sum("adjuster_level").alias("total_adjuster_level"),
    )
)
