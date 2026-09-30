from pyspark.sql import functions as F

valid = (
    df_claims
    .filter((F.col("approved_amount") >= 1364.5) & (F.year(F.col("claim_date")) == 2025))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        (F.col("approved_amount") - F.col("claimed_amount")).alias("approved_amount_minus_claimed_amount"),
    )
)

adjuster_id_stats = (
    valid
    .groupBy("adjuster_id")
    .agg(F.avg("approved_amount").alias("avg_approved_amount"))
)

above_average = (
    valid.alias("s")
    .join(adjuster_id_stats.alias("g"), F.col("g.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("s.approved_amount") > F.col("g.avg_approved_amount"))
    .select(
        "s.*",
        "g.avg_approved_amount",
        F.round(F.col("s.approved_amount") - F.col("g.avg_approved_amount"), 2).alias("approved_amount_minus_avg"),
    )
)

with_children_claims = (
    above_average.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.approved_amount",
        "s.avg_approved_amount",
        F.col("c.id").alias("claim_id"),
        F.col("c.claim_date").alias("claim_claim_date"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

combined_details_claim = (
    with_children_claims.alias("s")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "left")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.approved_amount",
        "s.claim_id",
        "s.claim_reported_at",
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.status").alias("claim_status"),
        F.col("c.reported_at").alias("claim_reported_at_2"),
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
    )
)

result = (
    combined_details_claim
    .groupingSets(
        [["claim_id", "adjuster_team", "adjuster_id"], ["claim_id"], []],
        "claim_id", "adjuster_team", "adjuster_id",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.countDistinct("claim_status").alias("distinct_claim_status"),
        F.round(F.stddev("claim_approved_amount"), 2).alias("stddev_claim_approved_amount"),
    )
)
