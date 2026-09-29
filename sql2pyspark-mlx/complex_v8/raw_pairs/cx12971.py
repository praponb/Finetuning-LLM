from pyspark.sql import functions as F

base_claims = (
    df_claims
    .filter(F.col("claim_date") >= F.lit("2024-07-01").cast("date"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
        F.when(F.col("status").isin("OPEN", "APPROVED"), "group_a").when(F.col("status") == "PAID", "group_b").otherwise("other").alias("status_group"),
        F.lower(F.trim(F.col("status"))).alias("status_clean"),
    )
)

annotated = (
    base_claims
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "reported_at_date",
        "status_group",
        "status_clean",
        F.when(F.col("status_group").isin("group_a", "other"), "group_a").when(F.col("status_group") == "group_b", "group_b").otherwise("other").alias("status_group_group"),
    )
)

result = (
    annotated
    .rollup("status_group", "policy_id", "status_group_group")
    .agg(
        F.countDistinct("policy_id").alias("distinct_policy_ids"),
        F.round(F.sum("claimed_amount"), 2).alias("total_claimed_amount"),
        F.datediff(F.max("reported_at_date"), F.min("reported_at_date")).alias("reported_at_date_span_days"),
    )
    .select(
        F.coalesce(F.col("status_group"), F.lit("ALL")).alias("status_group"),
        "policy_id",
        F.coalesce(F.col("status_group_group"), F.lit("ALL")).alias("status_group_group"),
        "distinct_policy_ids",
        "total_claimed_amount",
        "reported_at_date_span_days",
    )
    .orderBy(
        F.col("status_group").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
        F.col("status_group_group").asc_nulls_last(),
    )
)
