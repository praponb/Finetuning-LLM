from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_approved_amount_pr = Window.partitionBy("status").orderBy("approved_amount", "id").rowsBetween(-3, -1)
w_adjuster_id_policy_id = Window.partitionBy("adjuster_id", "policy_id")
w_policy_id_reported_at_year = Window.partitionBy("policy_id", F.year("reported_at"))
w_all_by_approved_amount_runni = Window.orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_reported_at = Window.orderBy("reported_at", "id")
w_status = Window.partitionBy("status")
w_all_by_claimed_amount_share = Window.orderBy(F.col("claimed_amount_share_of_policy_id_reported_at_ye").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_type_status_by_min_cla = Window.partitionBy("claim_type", "status").orderBy("min_claimed_amount_so_far", "id")

base = df_claims.filter(F.col("claim_date") >= F.lit("2024-04-11").cast("date"))

with_windows = (
    base
    .select(
        "*",
        F.max("approved_amount").over(w_status_by_approved_amount_pr).alias("approved_amount_max_3_prior"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_adjuster_id_policy_id), F.lit(0)), 4).alias("approved_amount_share_of_adjuster_id_policy_id"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_policy_id_reported_at_year), F.lit(0)), 4).alias("claimed_amount_share_of_policy_id_reported_at_ye"),
        F.round(F.avg("claimed_amount").over(w_policy_id_reported_at_year), 2).alias("policy_id_reported_at_year_avg_claimed_amount"),
        F.min("claimed_amount").over(w_all_by_approved_amount_runni).alias("min_claimed_amount_so_far"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_policy_id_reported_at_year), 2).alias("approved_amount_vs_policy_id_reported_at_year_av"),
        F.round(F.percent_rank().over(w_all_by_reported_at), 3).alias("reported_at_percent_rank"),
    )
)

derived = (
    with_windows
    .filter(F.col("policy_id_reported_at_year_avg_claimed_amount") <= 37525.0)
    .select(
        "id",
        "claim_type",
        "approved_amount",
        "status",
        "approved_amount_max_3_prior",
        "approved_amount_share_of_adjuster_id_policy_id",
        "claimed_amount_share_of_policy_id_reported_at_ye",
        "min_claimed_amount_so_far",
        "approved_amount_vs_policy_id_reported_at_year_av",
        "reported_at_percent_rank",
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
        F.concat(F.col("claim_type"), F.lit("#"), F.col("policy_id").cast("string")).alias("claim_type_ref"),
    )
)

with_windows_2 = (
    derived
    .select(
        "*",
        F.size(F.collect_set("claim_type").over(w_status)).alias("distinct_claim_type_in_status"),
        F.last("approved_amount").over(w_all_by_claimed_amount_share).alias("last_approved_amount"),
        F.round(F.col("reported_at_percent_rank") / F.nullif(F.sum("reported_at_percent_rank").over(w_status), F.lit(0)), 4).alias("reported_at_percent_rank_share_of_status"),
        (F.col("reported_at_percent_rank") - F.lag("reported_at_percent_rank").over(w_claim_type_status_by_min_cla)).alias("reported_at_percent_rank_change"),
    )
)

result = (
    with_windows_2
    .select(
        "id",
        "claim_type",
        "approved_amount",
        "status",
        "approved_amount_max_3_prior",
        "approved_amount_share_of_adjuster_id_policy_id",
        "claimed_amount_share_of_policy_id_reported_at_ye",
        "min_claimed_amount_so_far",
        "approved_amount_vs_policy_id_reported_at_year_av",
        "reported_at_date",
        "claim_type_ref",
        "distinct_claim_type_in_status",
        "last_approved_amount",
        "reported_at_percent_rank_share_of_status",
        "reported_at_percent_rank_change",
    )
    .orderBy(
        "claimed_amount_share_of_policy_id_reported_at_ye",
        "reported_at_percent_rank_share_of_status",
    )
)
