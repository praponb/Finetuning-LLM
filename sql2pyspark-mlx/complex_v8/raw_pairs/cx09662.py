from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("policy_id").orderBy("claim_date", "id")

scoped = (
    df_claims
    .filter(F.col("adjuster_id") <= 3)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        F.round(F.col("claimed_amount") * 1.5, 2).alias("claimed_amount_adj"),
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
    )
)

joined_policy = (
    scoped.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        "s.status",
        F.col("p.status").alias("policy_status"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.min("reported_at").alias("first_claim_reported_at"),
        F.min("claim_date").alias("first_claim_date"),
    )
)

with_stats = (
    joined_policy.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select("s.*", "c.first_claim_reported_at", "c.first_claim_date")
)

with_activity = (
    with_stats.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.claim_date").isNotNull()), "left_semi")
)

deduped = (
    with_activity
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_details_adjuster = (
    deduped.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        "s.status",
        "s.policy_status",
        "s.first_claim_reported_at",
        "s.first_claim_date",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
    )
)

result = (
    with_details_adjuster
    .cube("adjuster_id", "adjuster_team")
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.stddev("approved_amount"), 2).alias("stddev_approved_amount"),
    )
    .select(
        "adjuster_id",
        F.coalesce(F.col("adjuster_team"), F.lit("ALL")).alias("adjuster_team"),
        "row_count",
        "stddev_approved_amount",
    )
)
