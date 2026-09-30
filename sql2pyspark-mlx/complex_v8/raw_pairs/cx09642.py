from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_year = Window.partitionBy(F.year("claim_date"))
w_policy_id_reported_at_to_dat = Window.partitionBy("policy_id", F.to_date("reported_at"))
w_all_by_claimed_amount_ties = Window.orderBy(F.col("claimed_amount").desc())
w_claim_type_adjuster_id_by_cl = Window.partitionBy("claim_type", "adjuster_id").orderBy("claimed_amount")
w_claim_type_adjuster_id_by_cl_2 = Window.partitionBy("claim_type", "adjuster_id").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id_policy_id_by_day = Window.partitionBy("adjuster_id", "policy_id").orderBy("days_since_reported_at", "id")

prepared_claims = (
    df_claims
    .filter((F.col("status") == "REJECTED") & (F.col("claimed_amount") > 13635.1))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
        F.abs(F.col("claimed_amount") - 29985.9).alias("claimed_amount_distance"),
    )
)

ranked = (
    prepared_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "reported_at",
        "days_since_reported_at",
        "claimed_amount_distance",
        F.count(F.lit(1)).over(w_claim_date_year).alias("rows_in_claim_date_year"),
        F.round(F.col("claimed_amount_distance") / F.nullif(F.sum("claimed_amount_distance").over(w_claim_date_year), F.lit(0)), 4).alias("claimed_amount_distance_share_of_claim_date_year"),
        F.round(F.col("days_since_reported_at") - F.avg("days_since_reported_at").over(w_policy_id_reported_at_to_dat), 2).alias("days_since_reported_at_vs_policy_id_reported_at_"),
        F.dense_rank().over(w_all_by_claimed_amount_ties).alias("claimed_amount_dense_rank"),
        F.round(F.percent_rank().over(w_claim_type_adjuster_id_by_cl), 3).alias("claimed_amount_percent_rank"),
        F.nth_value("reported_at", 2).over(w_claim_type_adjuster_id_by_cl_2).alias("second_reported_at"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_policy_id_reported_at_to_dat), 2).alias("claimed_amount_vs_policy_id_reported_at_to_date_"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_adjuster_id_policy_id_by_day)).alias("days_since_prev_claim_date"),
    )
)

first_rows = (
    ranked
    .filter(F.col("claimed_amount_dense_rank") == 1)
    .drop("claimed_amount_dense_rank")
)

result = (
    first_rows
    .filter(F.col("adjuster_id").isNotNull())
    .select(
        "id",
        "adjuster_id",
        "reported_at",
        "days_since_reported_at",
        "claimed_amount_distance",
        "rows_in_claim_date_year",
        "claimed_amount_distance_share_of_claim_date_year",
        "days_since_reported_at_vs_policy_id_reported_at_",
        "second_reported_at",
        "claimed_amount_vs_policy_id_reported_at_to_date_",
        "days_since_prev_claim_date",
        (F.floor(F.col("days_since_reported_at") / 100) * 100).alias("days_since_reported_at_bucket"),
        F.last_day(F.col("claim_date")).alias("claim_date_month_end"),
    )
    .orderBy("reported_at")
)
