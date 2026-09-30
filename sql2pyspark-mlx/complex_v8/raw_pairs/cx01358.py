from pyspark.sql import functions as F

valid = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        F.when(F.col("status").isin("ACTIVE", "EXPIRED"), "group_a").when(F.col("status") == "LAPSED", "group_b").otherwise("other").alias("status_group"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.substring(F.trim(F.col("status")), 1, 4).alias("status_prefix"),
    )
)

line_level = (
    valid.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.adjuster_id").isNotNull()), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.end_date",
        "s.status_slug",
        "s.status_prefix",
        F.col("c.id").alias("claim_id"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        "c.claim_type",
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count(F.when(F.col("claim_type") != "theft", 1)).alias("claim_claim_type_count"),
        F.max("claimed_amount").alias("max_claim_claimed_amount"),
    )
)

summarized_rows = (
    line_level.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.claim_adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
        "c.max_claim_claimed_amount",
    )
)

claim_adjuster_id_stats = (
    summarized_rows
    .groupBy("claim_adjuster_id")
    .agg(F.avg("premium").alias("avg_premium"))
)

outliers = (
    summarized_rows.alias("s")
    .join(claim_adjuster_id_stats.alias("g"), F.col("g.claim_adjuster_id") == F.col("s.claim_adjuster_id"), "inner")
    .filter(F.col("s.premium") > F.col("g.avg_premium"))
    .select(
        "s.*",
        "g.avg_premium",
        F.round(F.col("s.premium") - F.col("g.avg_premium"), 2).alias("premium_minus_avg"),
    )
)

result = (
    outliers
    .groupBy("end_date", "claim_id")
    .pivot("claim_type", ["theft", "flood"])
    .agg(F.sum("claim_claim_type_count"))
    .orderBy("end_date", "claim_id")
)
