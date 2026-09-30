from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_by_claim_date_ti = Window.partitionBy("adjuster_id").orderBy(F.col("claim_date").desc())
w_claim_date_month_by_reported = Window.partitionBy(F.month("claim_date")).orderBy("reported_at_dayofweek", "id")
w_claim_date_month_by_reported_2 = Window.partitionBy(F.month("claim_date")).orderBy("reported_at_dayofweek")
w_adjuster_id_by_claim_date_mo = Window.partitionBy("adjuster_id").orderBy(F.col("claim_date").desc(), "id").rowsBetween(-6, Window.currentRow)
w_reported_at_dayofweek = Window.partitionBy("reported_at_dayofweek")
w_adjuster_id_by_claim_date = Window.partitionBy("adjuster_id").orderBy(F.col("claim_date").desc(), "id")
w_adjuster_id_by_claim_date_pr = Window.partitionBy("adjuster_id").orderBy(F.col("claim_date").desc(), "id").rowsBetween(-3, -1)
w_adjuster_id_by_reported_at_m = Window.partitionBy("adjuster_id").orderBy("reported_at", "id").rowsBetween(-2, Window.currentRow)
w_claim_date_month_by_reported_3 = Window.partitionBy(F.month("claim_date")).orderBy("reported_at_dayofweek", "id").rowsBetween(-6, Window.currentRow)
w_adjuster_id_by_reported_at_r = Window.partitionBy("adjuster_id").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_claims
    .filter((F.col("claimed_amount") < 31752.9) & (F.col("approved_amount") > 0.0))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.date_format(F.col("claim_date"), "yyyy-MM").alias("claim_date_label"),
        F.dayofweek(F.col("reported_at")).alias("reported_at_dayofweek"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status",
        "reported_at",
        "reported_at_dayofweek",
        F.round(F.cume_dist().over(w_adjuster_id_by_claim_date_ti), 3).alias("claim_date_cume_dist"),
        F.dense_rank().over(w_claim_date_month_by_reported).alias("reported_at_dayofweek_dense_rank"),
        F.row_number().over(w_claim_date_month_by_reported).alias("reported_at_dayofweek_seq"),
        F.round(F.percent_rank().over(w_claim_date_month_by_reported_2), 3).alias("reported_at_dayofweek_percent_rank"),
        F.sum("reported_at_dayofweek").over(w_adjuster_id_by_claim_date_mo).alias("reported_at_dayofweek_sum_7"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_reported_at_dayofweek), 2).alias("approved_amount_vs_reported_at_dayofweek_avg"),
        F.lag("reported_at_dayofweek").over(w_adjuster_id_by_claim_date).alias("prev_reported_at_dayofweek"),
        F.round(F.col("reported_at_dayofweek") - F.avg("reported_at_dayofweek").over(w_reported_at_dayofweek), 2).alias("reported_at_dayofweek_vs_reported_at_dayofweek_a"),
        F.max("approved_amount").over(w_adjuster_id_by_claim_date_pr).alias("approved_amount_max_3_prior"),
        F.round(F.avg("reported_at_dayofweek").over(w_adjuster_id_by_reported_at_m), 2).alias("reported_at_dayofweek_avg_3"),
        F.sum("reported_at_dayofweek").over(w_claim_date_month_by_reported_3).alias("reported_at_dayofweek_sum_7_2"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_reported_at_dayofweek), F.lit(0)), 4).alias("approved_amount_share_of_reported_at_dayofweek"),
        F.ntile(10).over(w_claim_date_month_by_reported).alias("reported_at_dayofweek_decile"),
        F.sum("approved_amount").over(w_adjuster_id_by_reported_at_r).alias("running_approved_amount"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "reported_at",
        "reported_at_dayofweek",
        "claim_date_cume_dist",
        "reported_at_dayofweek_dense_rank",
        "reported_at_dayofweek_percent_rank",
        "reported_at_dayofweek_sum_7",
        "approved_amount_vs_reported_at_dayofweek_avg",
        "approved_amount_max_3_prior",
        "reported_at_dayofweek_avg_3",
        "reported_at_dayofweek_sum_7_2",
        "reported_at_dayofweek_decile",
        "running_approved_amount",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
    .orderBy("reported_at_dayofweek_avg_3")
)
