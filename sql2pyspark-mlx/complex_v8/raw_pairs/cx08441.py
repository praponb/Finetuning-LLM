from pyspark.sql import functions as F

scoped_policys = (
    df_policies
    .filter(F.col("product").isin("LIFE", "AUTO", "HOME"))
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.sum("claimed_amount").alias("total_claim_claimed_amount"),
        F.min("claim_date").alias("first_claim_date"),
    )
)

with_stats = (
    scoped_policys.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select("s.*", "c.total_claim_claimed_amount", "c.first_claim_date")
)

rollup_base = (
    with_stats
    .groupBy("holder_id", "product")
    .agg(
        F.max("end_date").alias("last_end_date"),
        F.count(F.when(~F.col("status").isin("ACTIVE", "EXPIRED"), 1)).alias("n_status_not_in_active_expired"),
    )
)

result = (
    rollup_base
    .select(
        "holder_id",
        "product",
        "last_end_date",
        "n_status_not_in_active_expired",
        F.when(F.col("product").isin("HEALTH", "TRAVEL"), "group_a").when(F.col("product") == "LIFE", "group_b").otherwise("other").alias("product_group"),
        F.initcap(F.trim(F.col("product"))).alias("product_clean"),
    )
    .orderBy(F.col("n_status_not_in_active_expired").asc_nulls_last(), "product")
)
