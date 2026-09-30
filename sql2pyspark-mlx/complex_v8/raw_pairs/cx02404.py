from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_claim_type_by_claim_d = Window.partitionBy("status", "claim_type").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_type = Window.partitionBy("claim_type")
w_all_by_claim_date_month_prio = Window.orderBy(F.col("claim_date_month").asc_nulls_last(), "id").rowsBetween(-6, -1)
w_all_by_claim_date_month_runn = Window.orderBy(F.col("claim_date_month").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_claim_date_month_full = Window.orderBy(F.col("claim_date_month").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_claim_type_by_claim_d_2 = Window.partitionBy("status", "claim_type").orderBy("claim_date", "id")
w_all_by_claim_date_month = Window.orderBy(F.col("claim_date_month").asc_nulls_last(), "id")
w_policy_id_status_by_claim_da = Window.partitionBy("policy_id", "status").orderBy("claim_date_cume_dist", "id")
w_claim_date_month_by_claim_ty = Window.partitionBy("claim_date_month").orderBy(F.col("claim_type_avg_claimed_amount").desc(), "id")
w_reported_at_to_date_claim_da = Window.partitionBy(F.to_date("reported_at"), "claim_date_month")

cleaned = (
    df_claims
    .filter((F.col("claimed_amount") < 44921.3) & (F.col("claim_type") != "theft"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.month(F.col("claim_date")).alias("claim_date_month"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.min("claim_date_month").over(w_status_claim_type_by_claim_d).alias("min_claim_date_month_so_far"),
        F.round(F.col("claim_date_month") - F.avg("claim_date_month").over(w_claim_type), 2).alias("claim_date_month_vs_claim_type_avg"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_claim_type), 2).alias("claimed_amount_vs_claim_type_avg"),
        F.round(F.avg("claimed_amount").over(w_all_by_claim_date_month_prio), 2).alias("claimed_amount_avg_6_prior"),
        F.count(F.lit(1)).over(w_claim_type).alias("rows_in_claim_type"),
        F.last("claim_date", ignorenulls=True).over(w_all_by_claim_date_month_runn).alias("last_known_claim_date"),
        F.round(F.avg("claimed_amount").over(w_claim_type), 2).alias("claim_type_avg_claimed_amount"),
        F.first("reported_at").over(w_all_by_claim_date_month_full).alias("first_reported_at"),
        F.max("claim_date_month").over(w_claim_type).alias("claim_type_max_claim_date_month"),
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_status_claim_type_by_claim_d_2)).alias("claimed_amount_change"),
        F.round(F.cume_dist().over(w_status_claim_type_by_claim_d_2), 3).alias("claim_date_cume_dist"),
        F.row_number().over(w_all_by_claim_date_month).alias("claim_date_month_seq"),
    )
)

leaders = (
    metrics
    .filter(F.col("claim_date_month_seq") == 1)
    .drop("claim_date_month_seq")
)

sequenced = (
    leaders
    .select(
        "*",
        F.rank().over(w_policy_id_status_by_claim_da).alias("claim_date_cume_dist_rank"),
        F.lag("rows_in_claim_type", 2, 0).over(w_claim_date_month_by_claim_ty).alias("prev2_rows_in_claim_type"),
        F.round(F.percent_rank().over(w_claim_date_month_by_claim_ty), 3).alias("claim_type_avg_claimed_amount_percent_rank"),
        F.round(F.col("claimed_amount_avg_6_prior") - F.avg("claimed_amount_avg_6_prior").over(w_reported_at_to_date_claim_da), 2).alias("claimed_amount_avg_6_prior_vs_reported_at_to_dat"),
    )
)

result = (
    sequenced
    .filter(F.col("claimed_amount_change") >= -25000.0)
    .select("min_claim_date_month_so_far", "adjuster_id", "id")
    .distinct()
)
