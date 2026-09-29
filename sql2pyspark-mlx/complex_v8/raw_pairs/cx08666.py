from pyspark.sql import functions as F

filtered_claims = (
    df_claims
    .filter((F.col("policy_id") >= 23) | (F.col("adjuster_id") <= 4))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.last_day(F.col("claim_date")).alias("claim_date_month_end"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

summary = (
    filtered_claims
    .groupBy(
        "claim_type",
        "adjuster_id",
        F.date_format("claim_date_month_end", "yyyy-MM").alias("claim_date_month_end_ym"),
    )
    .agg(
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.count(F.when(F.col("claim_type") == "theft", 1)).alias("n_claim_type_theft"),
    )
)

scored = (
    summary
    .filter(F.col("adjuster_id").isNotNull())
    .select(
        "claim_type",
        "adjuster_id",
        "claim_date_month_end_ym",
        "status_list",
        F.coalesce(F.col("n_claim_type_theft"), F.lit(0)).alias("n_claim_type_theft_filled"),
        F.when(F.col("claim_type").isin("collision", "flood"), "group_a").when(F.col("claim_type") == "delay", "group_b").otherwise("other").alias("claim_type_group"),
    )
)

claim_type_group_stats = (
    scored
    .groupBy("claim_type_group")
    .agg(F.max("n_claim_type_theft_filled").alias("max_n_claim_type_theft_filled"))
)

outliers = (
    scored.alias("s")
    .join(claim_type_group_stats.alias("g"), F.col("g.claim_type_group") == F.col("s.claim_type_group"), "inner")
    .filter(F.col("s.n_claim_type_theft_filled") == F.col("g.max_n_claim_type_theft_filled"))
    .select(
        "s.*",
        "g.max_n_claim_type_theft_filled",
        F.round(F.col("s.n_claim_type_theft_filled") - F.col("g.max_n_claim_type_theft_filled"), 2).alias("n_claim_type_theft_filled_minus_max"),
    )
)

with_activity = (
    outliers.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left_semi")
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.max("claim_date").alias("last_claim_date"))
)

with_stats = (
    with_activity.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select("s.*", "c.last_claim_date")
)

result = (
    with_stats
    .cube("adjuster_id", "claim_type", "n_claim_type_theft_filled")
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("max_n_claim_type_theft_filled", 0.5).alias("median_max_n_claim_type_theft_filled"),
        F.countDistinct("claim_type_group").alias("distinct_claim_type_groups"),
    )
    .orderBy(
        F.col("adjuster_id").asc_nulls_last(),
        F.col("claim_type").asc_nulls_last(),
        F.col("n_claim_type_theft_filled").asc_nulls_last(),
    )
)
