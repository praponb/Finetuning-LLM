from pyspark.sql import functions as F

filtered = (
    df_policies
    .filter((F.col("product").isNotNull()) & (F.col("premium") < 3817.5))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.month(F.col("end_date")).alias("end_date_month"),
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
)

detail_rows = (
    filtered.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        F.col("c.id").alias("claim_id"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        "c.claim_date",
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

holder_id_stats = (
    detail_rows
    .groupBy("holder_id")
    .agg(F.percentile_approx("claim_approved_amount", 0.5).alias("median_claim_approved_amount"))
)

compared = (
    detail_rows.alias("s")
    .join(holder_id_stats.alias("g"), F.col("g.holder_id") == F.col("s.holder_id"), "inner")
    .filter(F.col("s.claim_approved_amount") >= F.col("g.median_claim_approved_amount"))
    .select(
        "s.*",
        "g.median_claim_approved_amount",
        F.round(F.col("s.claim_approved_amount") - F.col("g.median_claim_approved_amount"), 2).alias("claim_approved_amount_minus_median"),
    )
)

scored = (
    compared
    .select(
        "*",
        F.coalesce(F.col("claim_approved_amount"), F.lit(0.0)).alias("claim_approved_amount_filled"),
        F.abs(F.col("claim_approved_amount") - 14595.6).alias("claim_approved_amount_distance"),
        (F.floor(F.col("claim_approved_amount") / 10000) * 10000).alias("claim_approved_amount_bucket"),
    )
)

scored_2 = (
    scored
    .select(
        "id",
        "start_date",
        "end_date",
        "claim_id",
        "claim_adjuster_id",
        "claim_approved_amount",
        "median_claim_approved_amount",
        "claim_approved_amount_minus_median",
        F.round(F.col("claim_approved_amount") * 0.25, 2).alias("claim_approved_amount_adj"),
        F.round(F.col("claim_approved_amount") * 0.25, 2).alias("claim_approved_amount_adj_2"),
        (F.floor(F.col("claim_approved_amount") / 10000) * 10000).alias("claim_approved_amount_bucket_2"),
    )
)

enriched_adjuster = (
    scored_2.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.claim_adjuster_id"), "inner")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "left")
    .filter(F.col("a.team") == "Motor")
    .select(
        "s.id",
        "s.start_date",
        "s.end_date",
        "s.claim_id",
        "s.claim_adjuster_id",
        "s.claim_approved_amount",
        "s.claim_approved_amount_minus_median",
        "s.claim_approved_amount_adj",
        "s.claim_approved_amount_adj_2",
        "s.claim_approved_amount_bucket_2",
        F.col("a.team").alias("adjuster_team"),
        "c.claim_date",
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
    )
)

result = (
    enriched_adjuster
    .filter(F.col("claim_approved_amount_adj_2") < 60000.0)
    .select(
        "id",
        "end_date",
        "claim_id",
        "claim_approved_amount",
        "claim_approved_amount_minus_median",
        "claim_approved_amount_adj",
        "claim_approved_amount_bucket_2",
        "adjuster_team",
        "claim_claimed_amount",
        F.round(F.col("claim_approved_amount") * 1.5, 2).alias("claim_approved_amount_adj_3"),
    )
    .orderBy(F.col("claim_claimed_amount").desc())
)
