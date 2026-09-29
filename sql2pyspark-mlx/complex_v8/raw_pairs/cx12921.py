from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_claim_type_by_re = Window.partitionBy("adjuster_id", "claim_type").orderBy(F.col("reported_at").desc(), "id")
w_status_policy_holder_id = Window.partitionBy("status", "policy_holder_id")
w_all_by_claim_date = Window.orderBy("claim_date", "id")

cleaned = (
    df_claims
    .filter(F.col("reported_at") >= F.lit("2025-01-31 13:00:00").cast("timestamp"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        (F.col("reported_at") + F.expr("INTERVAL 7 DAYS")).alias("reported_at_plus_7d"),
    )
)

status_stats = (
    cleaned
    .groupBy("status")
    .agg(F.avg("approved_amount").alias("avg_approved_amount"))
)

outliers = (
    cleaned.alias("s")
    .join(status_stats.alias("g"), F.col("g.status") == F.col("s.status"), "inner")
    .filter(F.col("s.approved_amount") > F.col("g.avg_approved_amount"))
    .select(
        "s.*",
        "g.avg_approved_amount",
        F.round(F.col("s.approved_amount") - F.col("g.avg_approved_amount"), 2).alias("approved_amount_minus_avg"),
    )
)

with_parent_policy = (
    outliers.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.*",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.status").alias("policy_status"),
    )
)

metrics = (
    with_parent_policy
    .select(
        "*",
        F.ntile(10).over(w_adjuster_id_claim_type_by_re).alias("reported_at_decile"),
        F.size(F.collect_set("adjuster_id").over(w_status_policy_holder_id)).alias("distinct_adjuster_id_in_status_policy_holder_id"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_all_by_claim_date)).alias("days_since_prev_claim_date"),
        F.round(F.avg("claimed_amount").over(w_status_policy_holder_id), 2).alias("status_policy_holder_id_avg_claimed_amount"),
    )
)

annotated = (
    metrics
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        "reported_at_plus_7d",
        "avg_approved_amount",
        "approved_amount_minus_avg",
        "policy_end_date",
        "policy_holder_id",
        "distinct_adjuster_id_in_status_policy_holder_id",
        "days_since_prev_claim_date",
        F.coalesce(F.col("policy_status"), F.lit("UNKNOWN")).alias("policy_status_or_unknown"),
    )
)

result = (
    annotated
    .filter(F.col("approved_amount") >= 0.0)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "reported_at_plus_7d",
        "policy_end_date",
        "policy_holder_id",
        "distinct_adjuster_id_in_status_policy_holder_id",
        "policy_status_or_unknown",
    )
)
