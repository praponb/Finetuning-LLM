from pyspark.sql import functions as F

filtered = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
        F.quarter(F.col("reported_at")).alias("reported_at_quarter"),
    )
)

totals = (
    filtered
    .filter(F.col("reported_at_quarter") < 3)
    .groupBy(
        F.date_format("reported_at", "yyyy-MM").alias("reported_at_ym"),
        "reported_at_quarter",
    )
    .agg(
        F.round(F.sum("approved_amount") / F.nullif(F.sum("claimed_amount"), F.lit(0)), 3).alias("approved_amount_to_claimed_amount_ratio"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("claimed_amount").alias("sum_claimed_amount_check"),
    )
    .filter(F.col("sum_claimed_amount_check") > 10518.4)
    .drop("sum_claimed_amount_check")
)

without_matches = (
    totals
    .crossJoin(totals.agg(F.min("approved_amount_to_claimed_amount_ratio").alias("overall_min_approved_amount_to_claimed_amount_ra")))
    .filter(F.col("approved_amount_to_claimed_amount_ratio") > F.col("overall_min_approved_amount_to_claimed_amount_ra"))
    .drop("overall_min_approved_amount_to_claimed_amount_ra")
)

scored = (
    without_matches
    .filter(F.col("approved_amount_to_claimed_amount_ratio") < 9.0)
    .select("*", F.abs(F.col("distinct_ids") - 6).alias("distinct_ids_distance"))
)

screened = (
    scored
    .crossJoin(scored.agg(F.min("distinct_ids_distance").alias("overall_min_distinct_ids_distance")))
    .filter(F.col("distinct_ids_distance") > F.col("overall_min_distinct_ids_distance"))
    .drop("overall_min_distinct_ids_distance")
)

derived = (
    screened
    .select(
        "reported_at_ym",
        "reported_at_quarter",
        "distinct_ids_distance",
        (F.floor(F.col("distinct_ids_distance") / 2) * 2).alias("distinct_ids_distance_bucket"),
        (F.floor(F.col("distinct_ids") / 2) * 2).alias("distinct_ids_bucket"),
        F.length(F.trim(F.col("reported_at_ym"))).alias("reported_at_ym_len"),
    )
)

result = (
    derived
    .filter(F.col("distinct_ids_distance") < 8)
    .select(
        "reported_at_ym",
        "reported_at_quarter",
        "distinct_ids_distance",
        "distinct_ids_bucket",
    )
    .orderBy(F.col("distinct_ids_bucket").desc_nulls_first(), "reported_at_ym")
)
