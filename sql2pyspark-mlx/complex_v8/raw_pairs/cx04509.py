from pyspark.sql import functions as F

filtered_policys = (
    df_policies
    .filter((F.year(F.col("start_date")) == 2025) & (F.col("status") != "LAPSED"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
        F.datediff(F.col("start_date"), F.col("end_date")).alias("days_end_date_to_start_date"),
    )
)

screened = (
    filtered_policys.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left_semi")
    .filter(F.col("product") == "LIFE")
)

with_children = (
    screened.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.premium_filled",
        "s.days_end_date_to_start_date",
        F.col("c.id").alias("claim_id"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.status").alias("claim_status"),
    )
)

result = (
    with_children
    .rollup("claim_adjuster_id", "holder_id")
    .agg(
        F.percentile_approx("premium_filled", 0.5).alias("median_premium_filled"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("claim_status").isin("PAID", "REJECTED"), 1)).alias("n_claim_status_in_paid_rejecte"),
    )
    .orderBy(
        F.col("claim_adjuster_id").asc_nulls_last(),
        F.col("holder_id").asc_nulls_last(),
    )
)
