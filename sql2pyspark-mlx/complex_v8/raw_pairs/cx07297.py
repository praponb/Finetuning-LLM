from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_claim_type = Window.partitionBy("status", "claim_type")
w_all_by_days_since_claim_date = Window.orderBy(F.col("days_since_claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_days_since_claim_date_2 = Window.orderBy(F.col("days_since_claim_date").desc(), "id")
w_status_adjuster_id_by_claim = Window.partitionBy("status", "adjuster_id").orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_days_since_claim_date_3 = Window.orderBy(F.col("days_since_claim_date").desc())
w_adjuster_id_by_claim_date_ye = Window.partitionBy("adjuster_id").orderBy(F.col("claim_date_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id_by_claim_date_ye_2 = Window.partitionBy("adjuster_id").orderBy(F.col("claim_date_year").desc())
w_status_adjuster_id_by_claim_2 = Window.partitionBy("status", "adjuster_id").orderBy(F.col("claim_date").desc(), "id")

recent_claims = (
    df_claims
    .filter(F.col("approved_amount") < 15477.7)
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
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
        F.date_trunc("year", F.col("claim_date")).alias("claim_date_year"),
    )
)

ranked = (
    recent_claims
    .select(
        "*",
        F.round(F.col("days_since_claim_date") / F.nullif(F.sum("days_since_claim_date").over(w_status_claim_type), F.lit(0)), 4).alias("days_since_claim_date_share_of_status_claim_type"),
        F.nth_value("claimed_amount", 2).over(w_all_by_days_since_claim_date).alias("second_claimed_amount"),
        F.sum("days_since_claim_date").over(w_status_claim_type).alias("status_claim_type_sum_days_since_claim_date"),
        F.row_number().over(w_all_by_days_since_claim_date_2).alias("seq_no"),
        F.lead("reported_at").over(w_all_by_days_since_claim_date_2).alias("next_reported_at"),
        F.first("claim_date").over(w_status_adjuster_id_by_claim).alias("first_claim_date"),
        F.dense_rank().over(w_all_by_days_since_claim_date_3).alias("days_since_claim_date_dense_rank"),
        F.max("days_since_claim_date").over(w_status_claim_type).alias("status_claim_type_max_days_since_claim_date"),
        F.max("claimed_amount").over(w_adjuster_id_by_claim_date_ye).alias("max_claimed_amount_so_far"),
        F.dense_rank().over(w_adjuster_id_by_claim_date_ye_2).alias("claim_date_year_dense_rank"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_status_claim_type), 2).alias("approved_amount_vs_status_claim_type_avg"),
        F.row_number().over(w_status_adjuster_id_by_claim_2).alias("seq_no_2"),
        F.round(F.percent_rank().over(w_adjuster_id_by_claim_date_ye_2), 3).alias("claim_date_year_percent_rank"),
        F.count(F.lit(1)).over(w_status_claim_type).alias("rows_in_status_claim_type"),
    )
)

leaders = (
    ranked
    .filter((F.col("seq_no") <= 5) & (F.col("status_claim_type_sum_days_since_claim_date") >= 1050))
)

result = (
    leaders
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        "days_since_claim_date",
        "claim_date_year",
        "days_since_claim_date_share_of_status_claim_type",
        "second_claimed_amount",
        "status_claim_type_sum_days_since_claim_date",
        "seq_no",
        "next_reported_at",
        "first_claim_date",
        "days_since_claim_date_dense_rank",
        "status_claim_type_max_days_since_claim_date",
        "max_claimed_amount_so_far",
        "claim_date_year_dense_rank",
        "approved_amount_vs_status_claim_type_avg",
        "seq_no_2",
        "claim_date_year_percent_rank",
        "rows_in_status_claim_type",
        F.least(F.col("second_claimed_amount"), F.col("approved_amount")).alias("least_second_claimed_amount_approved_amount"),
    )
)
