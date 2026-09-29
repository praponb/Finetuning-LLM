from pyspark.sql import functions as F

valid = (
    df_claims
    .filter(F.col("reported_at") >= F.lit("2025-01-25 13:00:00").cast("timestamp"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.greatest(F.col("approved_amount"), F.col("claimed_amount")).alias("greatest_approved_amount_claimed_amount"),
    )
)

adjuster_id_stats = (
    valid
    .groupBy("adjuster_id")
    .agg(F.avg("claimed_amount").alias("avg_claimed_amount"))
)

above_average = (
    valid.alias("s")
    .join(adjuster_id_stats.alias("g"), F.col("g.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("s.claimed_amount") > F.col("g.avg_claimed_amount"))
    .select(
        "s.*",
        "g.avg_claimed_amount",
        F.round(F.col("s.claimed_amount") - F.col("g.avg_claimed_amount"), 2).alias("claimed_amount_minus_avg"),
    )
)

enriched_adjuster = (
    above_average.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.status_slug",
        "s.greatest_approved_amount_claimed_amount",
        "s.avg_claimed_amount",
        "s.claimed_amount_minus_avg",
        F.col("a.team").alias("adjuster_team"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

qualified = (
    enriched_adjuster.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.claim_date") >= F.lit("2024-03-24").cast("date")), "left_anti")
)

result = (
    qualified
    .select(
        "id",
        "claimed_amount_minus_avg",
        "approved_amount",
        "greatest_approved_amount_claimed_amount",
    )
    .unpivot(
        ids=["id"],
        values=["claimed_amount_minus_avg", "approved_amount", "greatest_approved_amount_claimed_amount"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
