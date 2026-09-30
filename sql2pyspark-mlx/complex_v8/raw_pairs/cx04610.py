from pyspark.sql import functions as F

scoped = (
    df_policies
    .filter((F.col("holder_id") >= 6) | (F.col("start_date") < F.lit("2025-03-24").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.datediff(F.col("start_date"), F.col("end_date")).alias("days_end_date_to_start_date"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.when(F.col("product").isin("HOME", "AUTO"), "group_a").when(F.col("product") == "LIFE", "group_b").otherwise("other").alias("product_group"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.sum("approved_amount").alias("total_claim_approved_amount"),
        F.count("*").alias("claim_count"),
        F.sum("approved_amount").alias("total_claim_approved_amount_2"),
    )
)

with_counts = (
    scoped.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "c.total_claim_approved_amount",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.total_claim_approved_amount_2",
    )
)

qualified = (
    with_counts.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.premium") < 4417.0), "left_anti")
)

tagged = (
    qualified
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        "days_end_date_to_start_date",
        "status_slug",
        "total_claim_approved_amount",
        "claim_count",
        F.year(F.col("start_date")).alias("start_date_year"),
    )
)

grouped = (
    tagged
    .filter(F.col("holder_id").isNotNull())
    .groupBy("status", "product", "start_date_year")
    .agg(
        F.count(F.when(F.col("status").isNotNull(), 1)).alias("n_status_is_not_null"),
        F.sum("claim_count").alias("total_claim_count"),
        F.sum(F.when(F.col("status") == "ACTIVE", 1).otherwise(0)).alias("n_status_active"),
        F.count(F.when(F.col("status").isin("LAPSED", "EXPIRED", "CANCELLED"), 1)).alias("n_status_in_lapsed_expired_can"),
    )
)

result = (
    grouped
    .select(
        "status",
        "product",
        "start_date_year",
        "n_status_is_not_null",
        "n_status_in_lapsed_expired_can",
    )
    .orderBy(
        F.col("product").desc(),
        F.col("status").desc_nulls_first(),
        "start_date_year",
    )
    .limit(20)
)
