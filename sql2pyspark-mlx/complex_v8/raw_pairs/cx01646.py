from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_policy_id_by_approved = Window.partitionBy("status", "policy_id").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_status = Window.partitionBy("policy_id", "status")
w_status_policy_id_by_approved_2 = Window.partitionBy("status", "policy_id").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_claims
    .filter(F.col("adjuster_id") >= 6)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.concat_ws(" / ", F.col("claim_type"), F.col("status")).alias("claim_type_status_label"),
        F.least(F.col("approved_amount"), F.col("claimed_amount")).alias("least_approved_amount_claimed_amount"),
    )
)

enriched = scoped.withColumn("claim_date_plus_365d", F.date_add(F.col("claim_date"), 365))

with_windows = (
    enriched
    .select(
        "*",
        F.sum("approved_amount").over(w_status_policy_id_by_approved).alias("running_approved_amount"),
        F.count(F.lit(1)).over(w_status_policy_id_by_approved).alias("running_count"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_policy_id_status), F.lit(0)), 4).alias("approved_amount_share_of_policy_id_status"),
        F.last("claim_date_plus_365d").over(w_status_policy_id_by_approved_2).alias("last_claim_date_plus_365d"),
    )
)

without_matches = (
    with_windows
    .crossJoin(with_windows.agg(F.max("running_count").alias("overall_max_running_count")))
    .filter(F.col("running_count") < F.col("overall_max_running_count"))
    .drop("overall_max_running_count")
)

with_parent_policy = (
    without_matches.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.least_approved_amount_claimed_amount",
        "s.claim_date_plus_365d",
        "s.running_approved_amount",
        "s.last_claim_date_plus_365d",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.status").alias("policy_status"),
        F.col("p.premium").alias("policy_premium"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.min("reported_at").alias("first_claim_reported_at"))
)

with_counts = (
    with_parent_policy.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select("s.*", "c.first_claim_reported_at")
)

line_level = (
    with_counts.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.approved_amount") < 32511.8), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.approved_amount",
        "s.reported_at",
        "s.least_approved_amount_claimed_amount",
        "s.running_approved_amount",
        "s.last_claim_date_plus_365d",
        "s.policy_holder_id",
        "s.policy_premium",
        "s.first_claim_reported_at",
        F.col("c.id").alias("claim_id"),
        "c.claim_date",
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
    )
)

result = (
    line_level
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "policy_holder_id",
        "policy_premium",
        "claim_date",
        "claim_approved_amount",
        "claim_reported_at",
        "claim_adjuster_id",
        (F.col("policy_premium") - F.col("approved_amount")).alias("policy_premium_minus_approved_amount"),
        F.concat(F.col("claim_type"), F.lit("#"), F.col("adjuster_id").cast("string")).alias("claim_type_ref"),
    )
    .orderBy(F.col("claim_adjuster_id").desc(), F.col("policy_holder_id").desc())
)
