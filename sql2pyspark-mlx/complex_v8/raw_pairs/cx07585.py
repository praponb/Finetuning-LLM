from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_approved_amount = Window.orderBy(F.col("approved_amount").desc(), "id")
w_claim_type = Window.partitionBy("claim_type")
w_adjuster_id_by_reported_at = Window.partitionBy("adjuster_id").orderBy(F.col("reported_at").desc(), "id")
w_status = Window.partitionBy("status")
w_claim_date_month_adjuster_id = Window.partitionBy(F.date_trunc("month", "claim_date"), "adjuster_id").orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_date_month = Window.partitionBy(F.date_trunc("month", "claim_date"))

recent_claims = (
    df_claims
    .filter((F.col("status") != "IN_REVIEW") & (F.col("claim_type").isin("medical", "collision")))
)

with_parent_policy = (
    recent_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        F.col("p.holder_id").alias("policy_holder_id"),
    )
)

windowed = (
    with_parent_policy
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_approved_amount), 3).alias("approved_amount_percent_rank"),
        F.ntile(10).over(w_all_by_approved_amount).alias("approved_amount_decile"),
        F.size(F.collect_set("policy_id").over(w_claim_type)).alias("distinct_policy_id_in_claim_type"),
        F.lag("reported_at").over(w_adjuster_id_by_reported_at).alias("prev_reported_at"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_status), F.lit(0)), 4).alias("approved_amount_share_of_status"),
        F.nth_value("approved_amount", 2).over(w_claim_date_month_adjuster_id).alias("second_approved_amount"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_status), F.lit(0)), 4).alias("claimed_amount_share_of_status"),
        F.count(F.lit(1)).over(w_claim_date_month).alias("rows_in_claim_date_month"),
    )
)

stats = (
    windowed
    .groupBy("policy_id", "approved_amount_decile", "adjuster_id")
    .agg(
        F.round(F.sum("approved_amount") / F.nullif(F.sum("approved_amount_decile"), F.lit(0)), 3).alias("approved_amount_to_approved_amount_decile_ratio"),
        F.min("claim_date").alias("first_claim_date"),
        F.avg("claimed_amount_share_of_status").alias("avg_claimed_amount_share_of_status_check"),
    )
    .filter(F.col("avg_claimed_amount_share_of_status_check") > 0.3)
    .drop("avg_claimed_amount_share_of_status_check")
)

result = (
    stats
    .filter(F.col("approved_amount_decile") < 7)
    .select(
        "policy_id",
        "approved_amount_decile",
        "adjuster_id",
        "approved_amount_to_approved_amount_decile_ratio",
        "first_claim_date",
        F.month(F.col("first_claim_date")).alias("first_claim_date_month"),
        F.date_trunc("week", F.col("first_claim_date")).alias("first_claim_date_week"),
    )
    .orderBy("first_claim_date_month", F.col("approved_amount_decile").desc())
)
