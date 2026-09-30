from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_adjuster_id_by_cla = Window.partitionBy("policy_id", "adjuster_id").orderBy(F.col("claimed_amount_filled").desc(), "id").rowsBetween(-2, -1)
w_all_by_claimed_amount_runnin = Window.orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_date_year_by_reported = Window.partitionBy(F.year("claim_date")).orderBy(F.col("reported_at").desc(), "id")
w_all_by_claimed_amount = Window.orderBy("claimed_amount", "id")
w_adjuster_id = Window.partitionBy("adjuster_id")
w_policy_id_adjuster_id_by_cla_2 = Window.partitionBy("policy_id", "adjuster_id").orderBy(F.col("claimed_amount_filled").desc(), "id").rowsBetween(-2, Window.currentRow)
w_claim_date_year_by_reported_2 = Window.partitionBy(F.year("claim_date")).orderBy(F.col("reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_claimed_amount_ties = Window.orderBy("claimed_amount")

scoped = (
    df_claims
    .filter((F.col("status") == "APPROVED") | (F.col("claim_type").isin("theft", "delay", "flood")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
    )
)

with_windows = (
    scoped
    .select(
        "*",
        F.round(F.avg("claimed_amount").over(w_policy_id_adjuster_id_by_cla), 2).alias("claimed_amount_avg_2_prior"),
        F.min("claimed_amount_filled").over(w_all_by_claimed_amount_runnin).alias("min_claimed_amount_filled_so_far"),
        F.rank().over(w_claim_date_year_by_reported).alias("reported_at_rank"),
        F.row_number().over(w_all_by_claimed_amount).alias("rn"),
        F.min("claimed_amount").over(w_adjuster_id).alias("adjuster_id_min_claimed_amount"),
        F.lead("claimed_amount_filled").over(w_claim_date_year_by_reported).alias("next_claimed_amount_filled"),
        F.max("claimed_amount").over(w_policy_id_adjuster_id_by_cla_2).alias("claimed_amount_max_3"),
        F.lead("reported_at").over(w_claim_date_year_by_reported).alias("next_reported_at"),
        F.first("reported_at").over(w_claim_date_year_by_reported_2).alias("first_reported_at"),
        F.round(F.col("claimed_amount_filled") - F.avg("claimed_amount_filled").over(w_adjuster_id), 2).alias("claimed_amount_filled_vs_adjuster_id_avg"),
        F.sum("claimed_amount_filled").over(w_all_by_claimed_amount_runnin).alias("running_claimed_amount_filled"),
        F.round(F.cume_dist().over(w_all_by_claimed_amount_ties), 3).alias("claimed_amount_cume_dist"),
        F.lead("claim_date").over(w_all_by_claimed_amount).alias("next_claim_date"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_adjuster_id), F.lit(0)), 4).alias("claimed_amount_share_of_adjuster_id"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "policy_id",
        "claim_date",
        "status",
        "reported_at",
        "claimed_amount_filled",
        "claimed_amount_avg_2_prior",
        "min_claimed_amount_filled_so_far",
        "reported_at_rank",
        "rn",
        "adjuster_id_min_claimed_amount",
        "next_claimed_amount_filled",
        "next_reported_at",
        "first_reported_at",
        "claimed_amount_filled_vs_adjuster_id_avg",
        "running_claimed_amount_filled",
        "claimed_amount_cume_dist",
        "next_claim_date",
        F.abs(F.col("claimed_amount_cume_dist") - 0.7).alias("claimed_amount_cume_dist_distance"),
    )
    .orderBy(F.col("status").asc())
)
