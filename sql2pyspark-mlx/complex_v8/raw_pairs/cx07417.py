from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_status = Window.partitionBy("adjuster_id", "status")
w_all_by_claim_approved_amount = Window.orderBy("claim_approved_amount_2", "id", "claim_id").rowsBetween(-6, Window.currentRow)
w_all_by_claim_approved_amount_2 = Window.orderBy("claim_approved_amount_2", "id", "claim_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_reported_at_to_date_claim_id = Window.partitionBy(F.to_date("reported_at"), "claim_id").orderBy("reported_at", "id", "claim_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_claim_approved_amount_3 = Window.orderBy("claim_approved_amount_2", "id", "claim_id")

recent = (
    df_claims
    .filter(F.col("claimed_amount") >= 7204.1)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        "reported_at",
        F.round(F.col("approved_amount") / F.nullif(F.col("claimed_amount"), F.lit(0)), 3).alias("approved_amount_per_claimed_amount"),
        F.date_format(F.col("reported_at"), "yyyy-MM").alias("reported_at_label"),
    )
)

with_children = (
    recent.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.status",
        "s.reported_at",
        "s.approved_amount_per_claimed_amount",
        "s.reported_at_label",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.claim_date").alias("claim_claim_date"),
        F.col("c.status").alias("claim_status"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

with_details = (
    with_children.alias("s")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "inner")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("c.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.status",
        "s.reported_at",
        "s.approved_amount_per_claimed_amount",
        "s.reported_at_label",
        "s.claim_id",
        "s.claim_reported_at",
        "s.claim_claim_date",
        "s.claim_status",
        "s.claim_approved_amount",
        F.col("c.approved_amount").alias("claim_approved_amount_2"),
        F.col("c.reported_at").alias("claim_reported_at_2"),
        F.col("c.policy_id").alias("claim_policy_id"),
        F.col("a.team").alias("adjuster_team"),
    )
)

sequenced = (
    with_details
    .select(
        "id",
        "adjuster_id",
        "status",
        "reported_at",
        "approved_amount_per_claimed_amount",
        "reported_at_label",
        "claim_id",
        "claim_reported_at",
        "claim_status",
        "claim_approved_amount",
        "claim_policy_id",
        F.sum("claimed_amount").over(w_adjuster_id_status).alias("adjuster_id_status_sum_claimed_amount"),
        F.max("claim_approved_amount_2").over(w_all_by_claim_approved_amount).alias("claim_approved_amount_2_max_7"),
        F.last("claim_approved_amount").over(w_all_by_claim_approved_amount_2).alias("last_claim_approved_amount"),
        F.round(F.avg("claim_approved_amount").over(w_adjuster_id_status), 2).alias("adjuster_id_status_avg_claim_approved_amount"),
        F.last("claim_reported_at_2", ignorenulls=True).over(w_reported_at_to_date_claim_id).alias("last_known_claim_reported_at_2"),
        F.ntile(10).over(w_all_by_claim_approved_amount_3).alias("claim_approved_amount_2_decile"),
    )
)

result = (
    sequenced
    .rollup("claim_approved_amount_2_decile", "claim_id", "claim_policy_id")
    .agg(
        F.countDistinct("claim_status").alias("distinct_claim_status"),
        F.sum(F.when(~F.col("status").isin("IN_REVIEW", "OPEN"), 1).otherwise(0)).alias("n_status_not_in_in_review_open"),
    )
    .orderBy(
        F.col("claim_approved_amount_2_decile").asc_nulls_last(),
        F.col("claim_id").asc_nulls_last(),
        F.col("claim_policy_id").asc_nulls_last(),
    )
)
