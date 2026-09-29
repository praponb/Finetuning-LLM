from pyspark.sql import functions as F

prepared = (
    df_policies
    .filter(F.col("start_date") < F.lit("2025-05-30").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count(F.when(F.col("status").isin("APPROVED", "IN_REVIEW"), 1)).alias("claim_status_count"),
        F.count(F.when(F.col("status") == "PAID", 1)).alias("claim_status_count_2"),
    )
)

with_stats = (
    prepared.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
        F.coalesce(F.col("c.claim_status_count_2"), F.lit(0)).alias("claim_status_count_2"),
    )
)

derived = with_stats.withColumn("start_date_plus_7d", F.date_add(F.col("start_date"), 7))

joined_holder = (
    derived.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.premium_filled",
        "s.claim_status_count",
        "s.claim_status_count_2",
        F.col("p.region").alias("holder_region"),
        F.col("p.segment").alias("holder_segment"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

exploded_detail_claims = (
    joined_holder.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.reported_at") < F.lit("2025-02-24 07:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.premium_filled",
        "s.claim_status_count",
        "s.claim_status_count_2",
        "s.holder_region",
        "s.holder_segment",
        "s.holder_joined_on",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.reported_at").alias("claim_reported_at"),
        "c.claim_type",
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

rollup_base = (
    exploded_detail_claims
    .filter(F.col("premium_filled") < 4280.0)
    .groupBy(
        F.date_format("holder_joined_on", "yyyy-MM").alias("holder_joined_on_ym"),
        F.year("start_date").alias("start_date_year"),
    )
    .agg(
        F.array_sort(F.collect_set("holder_region")).alias("holder_region_list"),
        F.count("*").alias("row_count"),
        F.round(F.avg("premium"), 2).alias("avg_premium"),
    )
    .filter(F.col("row_count") >= 2)
)

qualified = (
    rollup_base
    .crossJoin(rollup_base.agg(F.min("avg_premium").alias("overall_min_avg_premium")))
    .filter(F.col("avg_premium") > F.col("overall_min_avg_premium"))
    .drop("overall_min_avg_premium")
)

result = (
    qualified
    .select(
        "holder_joined_on_ym",
        "start_date_year",
        "row_count",
        F.lower(F.trim(F.col("holder_joined_on_ym"))).alias("holder_joined_on_ym_clean"),
        F.array_join(F.array_sort(F.col("holder_region_list")), ",").alias("holder_region_list_text"),
    )
    .orderBy(
        F.col("holder_region_list_text").asc_nulls_last(),
        F.col("holder_joined_on_ym").desc(),
        "start_date_year",
    )
    .limit(10)
)
