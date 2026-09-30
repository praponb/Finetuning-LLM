from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_policy_start_date = Window.partitionBy("status").orderBy("policy_start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id_claim_type_by_po = Window.partitionBy("adjuster_id", "claim_type").orderBy(F.col("policy_start_date").desc(), "id")
w_claim_date_month_adjuster_id = Window.partitionBy(F.date_trunc("month", "claim_date"), "adjuster_id")
w_status_by_policy_start_date_2 = Window.partitionBy("status").orderBy("policy_start_date")
w_claim_type_adjuster_id = Window.partitionBy("claim_type", "adjuster_id")
w_all_by_policy_start_date_tie = Window.orderBy("policy_start_date")
w_claim_type_policy_start_date = Window.partitionBy("claim_type", F.date_trunc("month", "policy_start_date")).orderBy(F.col("claim_date").desc(), "id")
w_claim_type = Window.partitionBy("claim_type")
w_all_by_approved_amount = Window.orderBy(F.col("approved_amount").desc(), "id")

recent_claims = df_claims.filter((F.col("adjuster_id") >= 4) & (F.year(F.col("claim_date")) == 2025))

with_parent = (
    recent_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
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
        F.col("p.start_date").alias("policy_start_date"),
    )
)

ranked = (
    with_parent
    .select(
        "*",
        F.first("approved_amount").over(w_status_by_policy_start_date).alias("first_approved_amount"),
        F.ntile(3).over(w_adjuster_id_claim_type_by_po).alias("policy_start_date_tercile"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_claim_date_month_adjuster_id), 2).alias("claimed_amount_vs_claim_date_month_adjuster_id_a"),
        F.dense_rank().over(w_status_by_policy_start_date_2).alias("policy_start_date_dense_rank"),
        F.min("approved_amount").over(w_claim_type_adjuster_id).alias("claim_type_adjuster_id_min_approved_amount"),
        F.round(F.percent_rank().over(w_all_by_policy_start_date_tie), 3).alias("policy_start_date_percent_rank"),
        (F.col("approved_amount") - F.lag("approved_amount").over(w_claim_type_policy_start_date)).alias("approved_amount_change"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_claim_type), 2).alias("approved_amount_vs_claim_type_avg"),
        F.round(F.cume_dist().over(w_all_by_policy_start_date_tie), 3).alias("policy_start_date_cume_dist"),
        F.min("claimed_amount").over(w_claim_type_adjuster_id).alias("claim_type_adjuster_id_min_claimed_amount"),
        F.dense_rank().over(w_all_by_approved_amount).alias("approved_amount_dense_rank"),
    )
)

top_per_group = (
    ranked
    .filter((F.col("policy_start_date_dense_rank") <= 2) & (F.col("claim_type_adjuster_id_min_claimed_amount") > 125075.0))
    .drop("policy_start_date_dense_rank")
)

result = (
    top_per_group
    .filter(F.col("approved_amount_change") >= -15000.0)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        "policy_start_date",
        "first_approved_amount",
        "policy_start_date_tercile",
        "claim_type_adjuster_id_min_approved_amount",
        "policy_start_date_percent_rank",
        "approved_amount_change",
        "policy_start_date_cume_dist",
        "claim_type_adjuster_id_min_claimed_amount",
        "approved_amount_dense_rank",
        (F.col("claimed_amount") + F.col("first_approved_amount")).alias("claimed_amount_plus_first_approved_amount"),
        F.round(F.col("first_approved_amount") / F.nullif(F.col("approved_amount_change"), F.lit(0)), 3).alias("first_approved_amount_per_approved_amount_change"),
    )
    .orderBy(F.col("claimed_amount").desc_nulls_first(), "id")
    .limit(10)
)
