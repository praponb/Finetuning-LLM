from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("policy_id").orderBy(F.col("adjuster_level").desc(), F.col("id").desc())

recent_claims = (
    df_claims
    .filter((F.col("status") == "APPROVED") & (F.col("claim_date") < F.lit("2025-10-30").cast("date")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

combined_details = (
    recent_claims.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.upper(F.trim(F.col("a.adjuster_name"))).like("G%")), "inner")
    .filter(F.upper(F.trim(F.col("a.adjuster_name"))).like("G%"))
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.status_len",
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
    )
)

deduped = (
    combined_details
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

enriched = (
    deduped.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("a.level") <= 3)
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.adjuster_level",
        "s.adjuster_name",
        F.col("a.adjuster_name").alias("adjuster_adjuster_name"),
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level_2"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.min("reported_at").alias("first_claim_reported_at"),
        F.max("reported_at").alias("last_claim_reported_at"),
    )
)

with_stats = (
    enriched.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.first_claim_reported_at",
        "c.last_claim_reported_at",
    )
)

claim_stats_2 = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count(F.when(F.col("status").isin("APPROVED", "REJECTED", "IN_REVIEW"), 1)).alias("claim_status_count"),
        F.min("claim_date").alias("first_claim_date"),
        F.max("approved_amount").alias("max_claim_approved_amount"),
    )
)

summarized_rows = (
    with_stats.alias("s")
    .join(claim_stats_2.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
        "c.first_claim_date",
        "c.max_claim_approved_amount",
    )
)

result = (
    summarized_rows
    .groupBy("claim_count")
    .pivot("adjuster_team", ["Property", "Motor", "Travel"])
    .agg(F.min("claimed_amount").alias("min_v"), F.count("claimed_amount").alias("n"))
    .orderBy("claim_count")
)
