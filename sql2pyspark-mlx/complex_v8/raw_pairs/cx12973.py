from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_claim_type = Window.partitionBy("status", "claim_type")
w_all_by_approved_amount_dista = Window.orderBy("approved_amount_distance")
w_claim_date_month_adjuster_id = Window.partitionBy(F.date_trunc("month", "claim_date"), "adjuster_id").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_date_month_adjuster_id_2 = Window.partitionBy(F.date_trunc("month", "claim_date"), "adjuster_id").orderBy("claim_date", "id").rowsBetween(-3, Window.currentRow)
w_claim_date_month_by_approved = Window.partitionBy(F.month("claim_date")).orderBy("approved_amount", "id").rowsBetween(-1, -1)
w_all_by_approved_amount_dista_2 = Window.orderBy("approved_amount_distance", "id")
w_claim_date_month_by_approved_2 = Window.partitionBy(F.month("claim_date")).orderBy("approved_amount", "id")
w_policy_id_by_claim_date = Window.partitionBy("policy_id").orderBy(F.col("claim_date").desc(), "id")
w_claim_date_month_policy_id = Window.partitionBy(F.date_trunc("month", "claim_date"), "policy_id")

filtered_claims = (
    df_claims
    .filter((F.col("claimed_amount") > 19347.2) | (F.col("policy_id").isNotNull()))
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
        F.substring(F.trim(F.col("status")), 1, 3).alias("status_prefix"),
        F.abs(F.col("approved_amount") - 16404.6).alias("approved_amount_distance"),
    )
)

sequenced = (
    filtered_claims
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "status_prefix",
        "approved_amount_distance",
        F.round(F.col("approved_amount_distance") - F.avg("approved_amount_distance").over(w_status_claim_type), 2).alias("approved_amount_distance_vs_status_claim_type_av"),
        F.round(F.cume_dist().over(w_all_by_approved_amount_dista), 3).alias("approved_amount_distance_cume_dist"),
        F.last("claimed_amount").over(w_claim_date_month_adjuster_id).alias("last_claimed_amount"),
        F.sum("approved_amount_distance").over(w_claim_date_month_adjuster_id_2).alias("approved_amount_distance_sum_4"),
        F.sum("approved_amount_distance").over(w_claim_date_month_by_approved).alias("approved_amount_distance_sum_1_prior"),
        F.round(F.cume_dist().over(w_all_by_approved_amount_dista_2), 3).alias("approved_amount_distance_cume_dist_2"),
        F.ntile(5).over(w_claim_date_month_by_approved_2).alias("approved_amount_quintile"),
        F.ntile(5).over(w_policy_id_by_claim_date).alias("claim_date_quintile"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_claim_date_month_policy_id), F.lit(0)), 4).alias("approved_amount_share_of_claim_date_month_policy"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "approved_amount_distance",
        "approved_amount_distance_vs_status_claim_type_av",
        "approved_amount_distance_cume_dist",
        "last_claimed_amount",
        "approved_amount_distance_sum_1_prior",
        "approved_amount_quintile",
        "claim_date_quintile",
        "approved_amount_share_of_claim_date_month_policy",
        F.greatest(F.col("approved_amount_distance"), F.col("claim_date_quintile")).alias("greatest_approved_amount_distance_claim_date_qui"),
        F.substring(F.trim(F.col("status_prefix")), 1, 3).alias("status_prefix_prefix"),
    )
    .orderBy(F.col("last_claimed_amount").desc())
)
