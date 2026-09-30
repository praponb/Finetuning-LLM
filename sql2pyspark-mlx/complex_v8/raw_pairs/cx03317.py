from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claim_date = Window.orderBy(F.col("claim_date").desc_nulls_first(), "id")
w_claim_type_status_by_reporte = Window.partitionBy("claim_type", "status").orderBy("reported_at", "id")
w_status_by_reported_at_full = Window.partitionBy("status").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id_status_by_report = Window.partitionBy("adjuster_id", "status").orderBy(F.col("reported_at").asc_nulls_last(), "id")
w_status_by_reported_at_ties = Window.partitionBy("status").orderBy("reported_at")
w_claim_type_status_by_reporte_2 = Window.partitionBy("claim_type", "status").orderBy("reported_at")
w_status_by_reported_at = Window.partitionBy("status").orderBy("reported_at", "id")
w_claim_date_month_by_claim_da = Window.partitionBy(F.month("claim_date")).orderBy(F.col("claim_date").asc_nulls_last())
w_all_by_claim_date_full = Window.orderBy(F.col("claim_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_claim_date_ties = Window.orderBy(F.col("claim_date").desc())
w_claim_type_status_by_reporte_3 = Window.partitionBy("claim_type", "status").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_claims = (
    df_claims
    .filter(F.col("approved_amount") <= 23120.7)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "status",
        "reported_at",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

with_windows = (
    cleaned_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "status",
        "reported_at",
        F.lead("claim_date").over(w_all_by_claim_date).alias("next_claim_date"),
        F.row_number().over(w_claim_type_status_by_reporte).alias("rn"),
        F.lead("reported_at").over(w_claim_type_status_by_reporte).alias("next_reported_at"),
        F.nth_value("reported_at", 2).over(w_status_by_reported_at_full).alias("second_reported_at"),
        F.round(F.cume_dist().over(w_adjuster_id_status_by_report), 3).alias("reported_at_cume_dist"),
        F.ntile(3).over(w_adjuster_id_status_by_report).alias("reported_at_tercile"),
        F.round(F.percent_rank().over(w_status_by_reported_at_ties), 3).alias("reported_at_percent_rank"),
        F.round(F.percent_rank().over(w_claim_type_status_by_reporte_2), 3).alias("reported_at_percent_rank_2"),
        F.lag("claim_date", 2).over(w_status_by_reported_at).alias("prev2_claim_date"),
        F.round(F.percent_rank().over(w_claim_date_month_by_claim_da), 3).alias("claim_date_percent_rank"),
        F.last("reported_at").over(w_all_by_claim_date_full).alias("last_reported_at"),
        F.lead("reported_at", 3).over(w_all_by_claim_date).alias("next3_reported_at"),
        F.round(F.cume_dist().over(w_all_by_claim_date_ties), 3).alias("claim_date_cume_dist"),
        F.nth_value("claim_date", 2).over(w_claim_type_status_by_reporte_3).alias("second_claim_date"),
        F.round(F.percent_rank().over(w_all_by_claim_date_ties), 3).alias("claim_date_percent_rank_2"),
    )
)

result = (
    with_windows
    .filter(F.col("adjuster_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "status",
        "next_claim_date",
        "rn",
        "second_reported_at",
        "reported_at_cume_dist",
        "reported_at_percent_rank_2",
        "prev2_claim_date",
        "claim_date_percent_rank",
        "next3_reported_at",
        "claim_date_cume_dist",
        "second_claim_date",
    )
)
