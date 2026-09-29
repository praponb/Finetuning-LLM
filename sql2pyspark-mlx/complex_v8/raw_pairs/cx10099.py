from pyspark.sql import functions as F

prepared = (
    df_policies
    .filter((F.col("holder_id") >= 7) & (F.year(F.col("start_date")) == 2025))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.datediff(F.col("start_date"), F.col("end_date")).alias("days_end_date_to_start_date"),
        F.substring(F.trim(F.col("product")), 1, 2).alias("product_prefix"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.max("reported_at").alias("last_claim_reported_at"),
        F.count(F.when(F.col("status").isNotNull(), 1)).alias("claim_status_count"),
        F.min("claim_date").alias("first_claim_date"),
    )
)

with_counts = (
    prepared.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "c.last_claim_reported_at",
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
        "c.first_claim_date",
    )
)

enriched_holder = (
    with_counts.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.product_prefix",
        "s.last_claim_reported_at",
        "s.claim_status_count",
        "p.holder_name",
    )
)

detail_rows_claims = (
    enriched_holder.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.claim_status_count",
        "s.holder_name",
        F.col("c.id").alias("claim_id"),
        "c.claim_type",
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
    )
)

with_details_adjuster = (
    detail_rows_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.claim_adjuster_id"), "inner")
    .filter(F.col("a.team").isin("Property", "Health", "Motor"))
    .select(
        "s.*",
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
    )
)

enriched = (
    with_details_adjuster
    .filter(F.col("product") != "HOME")
    .select(
        "id",
        "holder_id",
        "claim_status_count",
        "claim_id",
        "claim_type",
        "claim_adjuster_id",
        "adjuster_team",
        "adjuster_name",
        "adjuster_level",
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
        F.coalesce(F.col("adjuster_level"), F.lit(0)).alias("adjuster_level_filled"),
    )
)

result = (
    enriched
    .select(
        "id",
        "claim_status_count",
        "claim_id",
        "claim_type",
        "adjuster_team",
        "adjuster_name",
        "adjuster_level",
        "claim_type_or_unknown",
        "adjuster_level_filled",
        F.concat(F.col("adjuster_name"), F.lit("#"), F.col("claim_status_count").cast("string")).alias("adjuster_name_ref"),
        F.initcap(F.trim(F.col("adjuster_name"))).alias("adjuster_name_clean"),
    )
    .orderBy(F.col("claim_id").asc())
)
