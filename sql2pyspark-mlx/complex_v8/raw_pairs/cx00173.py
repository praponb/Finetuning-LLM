from pyspark.sql import functions as F

filtered = (
    df_claims
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.when(F.col("claim_type").isin("flood", "medical"), "group_a").when(F.col("claim_type") == "fire", "group_b").otherwise("other").alias("claim_type_group"),
    )
)

filtered_2 = (
    filtered.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.adjuster_id").isNotNull()) & (F.col("c.claim_date") >= F.lit("2024-04-09").cast("date")), "left_anti")
)

annotated = (
    filtered_2
    .filter(F.to_date(F.col("reported_at")) < F.lit("2025-03-14").cast("date"))
    .select(
        "*",
        F.length(F.trim(F.col("claim_type_group"))).alias("claim_type_group_len"),
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
    )
)

combined_details = (
    annotated.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("a.level") >= 2)
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.claim_type_group",
        "s.claim_type_group_len",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
    )
)

adjuster_id_stats = (
    combined_details
    .groupBy("adjuster_id")
    .agg(F.avg("approved_amount").alias("avg_approved_amount"))
)

compared = (
    combined_details.alias("s")
    .join(adjuster_id_stats.alias("g"), F.col("g.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("s.approved_amount") > F.col("g.avg_approved_amount"))
    .select(
        "s.*",
        "g.avg_approved_amount",
        F.round(F.col("s.approved_amount") - F.col("g.avg_approved_amount"), 2).alias("approved_amount_minus_avg"),
    )
)

result = (
    compared
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "status",
        "reported_at",
        "claim_type_group",
        "claim_type_group_len",
        "adjuster_level",
        "adjuster_team",
        "avg_approved_amount",
        "approved_amount_minus_avg",
        F.concat(F.col("adjuster_team"), F.lit("#"), F.col("policy_id").cast("string")).alias("adjuster_team_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("claim_type_group")), " ", "_")).alias("claim_type_group_slug"),
    )
    .orderBy(F.col("adjuster_team").asc_nulls_last(), F.col("id").desc_nulls_first())
)
