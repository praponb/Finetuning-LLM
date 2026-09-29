from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id = Window.partitionBy("policy_id")
w_all_by_reported_at_quarter = Window.orderBy(F.col("reported_at_quarter").desc(), "id")
w_all_by_reported_at_quarter_f = Window.orderBy(F.col("reported_at_quarter").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_claims
    .filter((F.col("approved_amount") >= 0.0) & (~F.col("status").isin("IN_REVIEW", "APPROVED")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        F.date_trunc("quarter", F.col("reported_at")).alias("reported_at_quarter"),
        F.quarter(F.col("reported_at")).alias("reported_at_quarter_2"),
    )
)

with_details_adjuster = (
    cleaned.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.reported_at_quarter",
        "s.reported_at_quarter_2",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.max("claimed_amount").alias("max_claim_claimed_amount"),
        F.count("*").alias("claim_count"),
    )
)

with_stats = (
    with_details_adjuster.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        "c.max_claim_claimed_amount",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

sequenced = (
    with_stats
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "status",
        "reported_at_quarter",
        "reported_at_quarter_2",
        "adjuster_level",
        "adjuster_team",
        "adjuster_name",
        "claim_count",
        F.size(F.collect_set("status").over(w_policy_id)).alias("distinct_status_in_policy_id"),
        F.round(F.col("claim_count") - F.avg("claim_count").over(w_policy_id), 2).alias("claim_count_vs_policy_id_avg"),
        F.dense_rank().over(w_all_by_reported_at_quarter).alias("reported_at_quarter_dense_rank"),
        F.last("approved_amount").over(w_all_by_reported_at_quarter_f).alias("last_approved_amount"),
        F.lead("adjuster_level", 3).over(w_all_by_reported_at_quarter).alias("next3_adjuster_level"),
    )
)

result = (
    sequenced
    .groupBy("adjuster_id")
    .pivot("claim_type", ["collision", "medical"])
    .agg(F.sum("claim_count"))
)
