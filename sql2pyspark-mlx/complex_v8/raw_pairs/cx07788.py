from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_claim_type = Window.partitionBy("policy_id", "claim_type")
w_all_by_approved_amount_plus = Window.orderBy("approved_amount_plus_claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_claimed_amount_full = Window.orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_claims = (
    df_claims
    .filter((F.col("status") != "OPEN") & (F.col("claim_date") < F.lit("2024-12-22").cast("date")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
        (F.col("approved_amount") + F.col("claimed_amount")).alias("approved_amount_plus_claimed_amount"),
        F.lower(F.regexp_replace(F.trim(F.col("claim_type")), " ", "_")).alias("claim_type_slug"),
    )
)

metrics = (
    prepared_claims
    .select(
        "id",
        "policy_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "approved_amount_plus_claimed_amount",
        "claim_type_slug",
        F.size(F.collect_set("adjuster_id").over(w_policy_id_claim_type)).alias("distinct_adjuster_id_in_policy_id_claim_type"),
        F.last("claimed_amount").over(w_all_by_approved_amount_plus).alias("last_claimed_amount"),
        F.first("days_since_reported_at").over(w_all_by_claimed_amount_full).alias("first_days_since_reported_at"),
    )
)

with_activity = (
    metrics.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.premium") >= 2096.6) & (F.col("p.start_date") < F.lit("2025-03-23").cast("date")), "left_anti")
)

line_level = (
    with_activity.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.status") == "IN_REVIEW"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claimed_amount",
        "s.status",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.claim_type").alias("claim_claim_type"),
    )
)

with_parent_claim = (
    line_level.alias("s")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "left")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("c.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claimed_amount",
        "s.status",
        "s.claim_id",
        "s.claim_claim_type",
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
    )
)

joined_claim = (
    with_parent_claim.alias("s")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claimed_amount",
        "s.status",
        "s.claim_id",
        "s.claim_adjuster_id",
        "s.claim_claimed_amount",
        "s.adjuster_name",
        "s.adjuster_team",
        F.col("c.adjuster_id").alias("claim_adjuster_id_2"),
        F.col("c.policy_id").alias("claim_policy_id"),
    )
)

result = (
    joined_claim
    .filter(F.col("claim_adjuster_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "claimed_amount",
        "claim_id",
        "claim_adjuster_id",
        "claim_claimed_amount",
        "adjuster_name",
        "claim_adjuster_id_2",
    )
    .orderBy(F.col("id").desc(), "claimed_amount")
)
