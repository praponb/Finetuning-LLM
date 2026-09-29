from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id = Window.partitionBy("adjuster_id")
w_all_by_approved_amount = Window.orderBy("approved_amount", "id")
w_status_by_claim_date_quarter = Window.partitionBy("status").orderBy("claim_date_quarter", "id")
w_all_by_approved_amount_full = Window.orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_by_claim_date_quarter_2 = Window.partitionBy("status").orderBy("claim_date_quarter", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_claim_type = Window.partitionBy("status", "claim_type")

cleaned = (
    df_claims
    .filter(F.col("claim_date") < F.lit("2025-04-14").cast("date"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        F.quarter(F.col("claim_date")).alias("claim_date_quarter"),
        F.round(F.col("claimed_amount") * 1.5, 2).alias("claimed_amount_adj"),
    )
)

with_trends = (
    cleaned
    .select(
        "*",
        F.round(F.avg("approved_amount").over(w_adjuster_id), 2).alias("adjuster_id_avg_approved_amount"),
        F.lead("claim_date", 2).over(w_all_by_approved_amount).alias("next2_claim_date"),
        F.row_number().over(w_status_by_claim_date_quarter).alias("rn"),
        F.first("claim_date").over(w_all_by_approved_amount_full).alias("first_claim_date"),
        F.lead("claim_date").over(w_status_by_claim_date_quarter).alias("next_claim_date"),
        F.nth_value("approved_amount", 2).over(w_status_by_claim_date_quarter_2).alias("second_approved_amount"),
        F.nth_value("claimed_amount", 2).over(w_all_by_approved_amount_full).alias("second_claimed_amount"),
        F.min("claimed_amount_adj").over(w_status_claim_type).alias("status_claim_type_min_claimed_amount_adj"),
        F.row_number().over(w_all_by_approved_amount).alias("seq_no"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "status",
        "claim_date_quarter",
        "claimed_amount_adj",
        "adjuster_id_avg_approved_amount",
        "next2_claim_date",
        "rn",
        "first_claim_date",
        "next_claim_date",
        "second_approved_amount",
        "second_claimed_amount",
        "seq_no",
        F.round(F.col("approved_amount") / F.nullif(F.col("second_claimed_amount"), F.lit(0)), 3).alias("approved_amount_per_second_claimed_amount"),
    )
    .orderBy(F.col("policy_id").asc_nulls_last(), F.col("first_claim_date").asc())
)
