from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id = Window.partitionBy("policy_id")
w_policy_id_adjuster_id_by_rep = Window.partitionBy("policy_id", "adjuster_id").orderBy(F.col("reported_at").asc_nulls_last(), "id")

valid = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status",
        "reported_at",
        F.lower(F.regexp_replace(F.trim(F.col("claim_type")), " ", "_")).alias("claim_type_slug"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.max("approved_amount").over(w_policy_id).alias("policy_id_max_approved_amount"),
        F.rank().over(w_policy_id_adjuster_id_by_rep).alias("reported_at_rank"),
    )
)

joined = (
    metrics.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .filter(F.col("a.level") >= 1)
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.claim_type_slug",
        "s.policy_id_max_approved_amount",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count(F.when(F.col("claim_type") != "medical", 1)).alias("claim_claim_type_count"),
    )
)

profiled = (
    joined.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
    )
)

summary = (
    profiled
    .groupBy(
        "status",
        "policy_id",
        F.year("policy_end_date").alias("policy_end_date_year"),
    )
    .agg(
        F.sum(F.when(F.col("adjuster_team") == "Motor", F.col("adjuster_level"))).alias("adjuster_level_adjuster_team_motor"),
        F.min("claim_claim_type_count").alias("min_claim_claim_type_count"),
    )
)

with_children = (
    summary.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.status",
        "s.policy_id",
        "s.policy_end_date_year",
        "s.adjuster_level_adjuster_team_motor",
        "s.min_claim_claim_type_count",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        "c.claim_date",
    )
)

result = (
    with_children
    .groupBy("claim_reported_at", "policy_id")
    .pivot("status", ["PAID", "IN_REVIEW"])
    .agg(F.max("claim_approved_amount"))
)
