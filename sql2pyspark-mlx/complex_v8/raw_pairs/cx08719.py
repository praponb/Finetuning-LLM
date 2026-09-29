from pyspark.sql import functions as F

prepared_claims = (
    df_claims
    .filter((F.col("approved_amount").between(0.0, 20880.4)) & (F.col("status").isin("OPEN", "PAID")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("claim_type")), " ", "_")).alias("claim_type_slug"),
    )
)

adjuster_id_stats = (
    prepared_claims
    .groupBy("adjuster_id")
    .agg(F.percentile_approx("claimed_amount", 0.5).alias("median_claimed_amount"))
)

vs_group = (
    prepared_claims.alias("s")
    .join(adjuster_id_stats.alias("g"), F.col("g.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("s.claimed_amount") >= F.col("g.median_claimed_amount"))
    .select(
        "s.*",
        "g.median_claimed_amount",
        F.round(F.col("s.claimed_amount") - F.col("g.median_claimed_amount"), 2).alias("claimed_amount_minus_median"),
    )
)

with_parent_adjuster = (
    vs_group.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.claim_type_slug",
        "s.claimed_amount_minus_median",
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
    )
)

result = (
    with_parent_adjuster
    .select("id", "approved_amount", "claimed_amount_minus_median")
    .unpivot(
        ids=["id"],
        values=["approved_amount", "claimed_amount_minus_median"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
