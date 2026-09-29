from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_adjuster_id = Window.partitionBy("status", "adjuster_id")
w_adjuster_id_by_claimed_amoun = Window.partitionBy("adjuster_id").orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_claims
    .filter((F.col("claim_type") == "flood") & (F.col("approved_amount") > 2272.7))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        "reported_at",
        F.least(F.col("approved_amount"), F.col("claimed_amount")).alias("least_approved_amount_claimed_amount"),
        F.round(F.col("approved_amount") / F.nullif(F.col("claimed_amount"), F.lit(0)), 3).alias("approved_amount_per_claimed_amount"),
        (F.col("approved_amount") - F.col("claimed_amount")).alias("approved_amount_minus_claimed_amount"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "status",
        "reported_at",
        "least_approved_amount_claimed_amount",
        "approved_amount_minus_claimed_amount",
        F.count(F.lit(1)).over(w_status_adjuster_id).alias("rows_in_status_adjuster_id"),
        F.first("reported_at", ignorenulls=True).over(w_adjuster_id_by_claimed_amoun).alias("first_known_reported_at"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_status_adjuster_id), 2).alias("claimed_amount_vs_status_adjuster_id_avg"),
    )
)

qualified = (
    metrics.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left_semi")
)

enriched = (
    qualified.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.end_date").isNotNull()), "left")
    .select(
        "s.*",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.status").alias("policy_status"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.count("*").alias("claim_count"), F.count("*").alias("claim_count_2"))
)

with_stats = (
    enriched.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        F.coalesce(F.col("c.claim_count_2"), F.lit(0)).alias("claim_count_2"),
    )
)

line_level = (
    with_stats.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.reported_at",
        "s.least_approved_amount_claimed_amount",
        "s.approved_amount_minus_claimed_amount",
        "s.first_known_reported_at",
        "s.policy_end_date",
        "s.claim_count_2",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        "c.claim_type",
    )
)

result = (
    line_level
    .filter(F.col("policy_id") <= 24)
    .select(
        "id",
        "reported_at",
        "least_approved_amount_claimed_amount",
        "first_known_reported_at",
        "claim_count_2",
        "claim_id",
        "claim_type",
    )
    .orderBy("claim_count_2")
)
