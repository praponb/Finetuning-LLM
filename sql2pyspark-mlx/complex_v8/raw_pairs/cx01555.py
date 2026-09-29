from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reported_at_month_by_reporte = Window.partitionBy(F.month("reported_at")).orderBy("reported_at", "id")
w_adjuster_id_by_reported_at_f = Window.partitionBy("adjuster_id").orderBy(F.col("reported_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id_by_reported_at_r = Window.partitionBy("adjuster_id").orderBy(F.col("reported_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_reported_at_year_by_reported = Window.partitionBy(F.year("reported_at")).orderBy(F.col("reported_at").desc(), "id")
w_all_by_second_reported_at_fu = Window.orderBy(F.col("second_reported_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_reported_at_seq = Window.partitionBy("status", "reported_at_seq")
w_all_by_second_reported_at = Window.orderBy(F.col("second_reported_at").asc_nulls_last(), "id")

prepared = (
    df_claims
    .filter(F.col("claim_type") == "flood")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "status",
        "reported_at",
    )
)

metrics = (
    prepared
    .select(
        "*",
        F.row_number().over(w_reported_at_month_by_reporte).alias("reported_at_seq"),
        F.nth_value("reported_at", 2).over(w_adjuster_id_by_reported_at_f).alias("second_reported_at"),
        F.round(F.percent_rank().over(w_reported_at_month_by_reporte), 3).alias("reported_at_percent_rank"),
        F.last("reported_at", ignorenulls=True).over(w_adjuster_id_by_reported_at_r).alias("last_known_reported_at"),
        F.round((F.unix_timestamp("reported_at") - F.unix_timestamp(F.lag("reported_at").over(w_reported_at_year_by_reported))) / 60, 1).alias("minutes_since_prev_reported_at"),
    )
)

with_windows = (
    metrics
    .select(
        "*",
        F.nth_value("reported_at_seq", 2).over(w_all_by_second_reported_at_fu).alias("second_reported_at_seq"),
        F.round(F.col("reported_at_percent_rank") - F.avg("reported_at_percent_rank").over(w_status_reported_at_seq), 2).alias("reported_at_percent_rank_vs_status_reported_at_s"),
        F.round(F.col("reported_at_percent_rank") / F.nullif(F.sum("reported_at_percent_rank").over(w_status_reported_at_seq), F.lit(0)), 4).alias("reported_at_percent_rank_share_of_status_reporte"),
        F.row_number().over(w_all_by_second_reported_at).alias("seq_no"),
    )
)

result = (
    with_windows
    .rollup("reported_at_seq", "status")
    .agg(
        F.grouping("reported_at_seq").alias("is_total_reported_at_seq"),
        F.min("reported_at").alias("first_reported_at"),
    )
    .orderBy(
        F.col("reported_at_seq").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
    )
)
