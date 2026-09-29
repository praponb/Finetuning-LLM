from pyspark.sql import functions as F

filtered = (
    df_adjusters
    .filter((F.length(F.trim(F.col("adjuster_name"))) >= 7) & (F.col("team").isNotNull()))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.abs(F.col("level") - 3).alias("level_distance"),
    )
)

with_children = (
    filtered.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.id")) & (F.col("c.reported_at") < F.lit("2025-02-24 07:00:00").cast("timestamp")), "left")
    .select(
        "s.id",
        "s.adjuster_name",
        "s.team",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.sum("approved_amount").alias("total_claim_approved_amount"))
)

profiled = (
    with_children.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select("s.*", "c.total_claim_approved_amount")
)

team_stats = (
    profiled
    .groupBy("team")
    .agg(F.avg("total_claim_approved_amount").alias("avg_total_claim_approved_amount"))
)

above_average = (
    profiled.alias("s")
    .join(team_stats.alias("g"), F.col("g.team") == F.col("s.team"), "inner")
    .filter(F.col("s.total_claim_approved_amount") > F.col("g.avg_total_claim_approved_amount"))
    .select(
        "s.*",
        "g.avg_total_claim_approved_amount",
        F.round(F.col("s.total_claim_approved_amount") - F.col("g.avg_total_claim_approved_amount"), 2).alias("total_claim_approved_amount_minus_avg"),
    )
)

claim_stats_2 = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.max("reported_at").alias("last_claim_reported_at"))
)

summarized_rows = (
    above_average.alias("s")
    .join(claim_stats_2.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select("s.*", "c.last_claim_reported_at")
)

exploded_detail = (
    summarized_rows.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.adjuster_name",
        "s.team",
        "s.claim_id",
        "s.claim_reported_at",
        "s.avg_total_claim_approved_amount",
        "s.total_claim_approved_amount_minus_avg",
        "s.last_claim_reported_at",
        F.col("c.id").alias("claim_id_2"),
        "c.claim_type",
        F.col("c.policy_id").alias("claim_policy_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.status").alias("claim_status_2"),
    )
)

result = (
    exploded_detail
    .groupBy("team")
    .pivot("claim_type", ["fire", "delay"])
    .agg(F.sum("claim_approved_amount").alias("total"), F.count("claim_approved_amount").alias("n"))
)
