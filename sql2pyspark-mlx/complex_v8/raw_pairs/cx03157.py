from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_year_by_claimed_a = Window.partitionBy(F.year("claim_date")).orderBy(F.col("claimed_amount").desc(), "id")
w_status_reported_at_month_by = Window.partitionBy("status", F.month("reported_at")).orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_by_reported_at_prior = Window.partitionBy("status").orderBy("reported_at", "id").rowsBetween(-1, -1)
w_status_by_reported_at_ties = Window.partitionBy("status").orderBy("reported_at")
w_status = Window.partitionBy("status")
w_status_reported_at_month_by_2 = Window.partitionBy("status", F.month("reported_at")).orderBy(F.col("claimed_amount").desc())

scoped_claims = (
    df_claims
    .filter(F.col("adjuster_id") <= 6)
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
        F.concat_ws(" / ", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
        F.round(F.col("approved_amount") * 0.25, 2).alias("approved_amount_adj"),
    )
)

sequenced = (
    scoped_claims
    .select(
        "*",
        F.rank().over(w_claim_date_year_by_claimed_a).alias("claimed_amount_rank"),
        F.first("claimed_amount").over(w_status_reported_at_month_by).alias("first_claimed_amount"),
        F.sum("claimed_amount").over(w_status_by_reported_at_prior).alias("claimed_amount_sum_1_prior"),
        F.round(F.percent_rank().over(w_status_by_reported_at_ties), 3).alias("reported_at_percent_rank"),
        F.min("approved_amount_adj").over(w_status).alias("status_min_approved_amount_adj"),
        F.sum("approved_amount_adj").over(w_status).alias("status_sum_approved_amount_adj"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_status), 2).alias("claimed_amount_vs_status_avg"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_status), F.lit(0)), 4).alias("claimed_amount_share_of_status"),
        F.dense_rank().over(w_status_reported_at_month_by_2).alias("claimed_amount_dense_rank"),
    )
)

leaders = sequenced.filter(F.col("claimed_amount_rank") == 1)

result = (
    leaders
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        "approved_amount_adj",
        "claimed_amount_rank",
        "claimed_amount_sum_1_prior",
        "reported_at_percent_rank",
        "status_min_approved_amount_adj",
        "status_sum_approved_amount_adj",
        "claimed_amount_vs_status_avg",
        "claimed_amount_share_of_status",
        "claimed_amount_dense_rank",
    )
    .orderBy(
        F.col("claimed_amount_dense_rank").desc(),
        F.col("policy_id").desc(),
        "id",
    )
    .limit(20)
)
