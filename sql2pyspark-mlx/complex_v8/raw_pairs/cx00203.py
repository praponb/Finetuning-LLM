from pyspark.sql import functions as F

scoped_claims = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.least(F.col("approved_amount"), F.col("claimed_amount")).alias("least_approved_amount_claimed_amount"),
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
    )
)

labeled = (
    scoped_claims
    .select(
        "*",
        F.length(F.trim(F.col("claim_type_or_unknown"))).alias("claim_type_or_unknown_len"),
        F.date_format(F.col("claim_date"), "MMM yyyy").alias("claim_date_label"),
    )
)

claim_type_stats = (
    labeled
    .groupBy("claim_type")
    .agg(F.avg("claim_type_len").alias("avg_claim_type_len"))
)

above_average = (
    labeled.alias("s")
    .join(claim_type_stats.alias("g"), F.col("g.claim_type") == F.col("s.claim_type"), "inner")
    .filter(F.col("s.claim_type_len") > F.col("g.avg_claim_type_len"))
    .select(
        "s.*",
        "g.avg_claim_type_len",
        F.round(F.col("s.claim_type_len") - F.col("g.avg_claim_type_len"), 2).alias("claim_type_len_minus_avg"),
    )
)

without_matches = (
    above_average.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.level").between(1, 3)) & (F.col("a.adjuster_name").isNotNull()), "left_semi")
)

combined_details_adjuster = (
    without_matches.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.lower(F.col("a.adjuster_name")).like("%a %")), "inner")
    .select("s.*", F.col("a.team").alias("adjuster_team"))
)

labeled_2 = (
    combined_details_adjuster
    .select(
        "*",
        F.initcap(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
        (F.col("reported_at") + F.expr("INTERVAL 7 DAYS")).alias("reported_at_plus_7d"),
    )
)

result = (
    labeled_2
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        "least_approved_amount_claimed_amount",
        "claim_type_or_unknown",
        "claim_type_len",
        "claim_type_or_unknown_len",
        "claim_date_label",
        "avg_claim_type_len",
        "claim_type_len_minus_avg",
        "adjuster_team",
        "claim_type_clean",
        "claimed_amount_bucket",
        F.greatest(F.col("claimed_amount"), F.col("claim_type_len_minus_avg")).alias("greatest_claimed_amount_claim_type_len_minus_avg"),
    )
    .orderBy(
        F.col("claim_type_or_unknown_len").desc(),
        F.col("claim_date_label").asc_nulls_last(),
    )
)
