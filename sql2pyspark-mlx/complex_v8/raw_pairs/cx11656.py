from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_start_date_month_b = Window.partitionBy("holder_id", F.date_trunc("month", "start_date")).orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_segment = Window.partitionBy("holder_segment")
w_product_holder_id_by_end_dat = Window.partitionBy("product", "holder_id").orderBy(F.col("end_date").desc())
w_end_date_month_by_end_date_m = Window.partitionBy(F.date_trunc("month", "end_date")).orderBy(F.col("end_date").desc(), "id").rowsBetween(-3, Window.currentRow)
w_status_or_unknown = Window.partitionBy("status_or_unknown")
w_all_by_start_date = Window.orderBy("start_date", "id")
w_holder_id_start_date_month_b_2 = Window.partitionBy("holder_id", F.date_trunc("month", "start_date")).orderBy("end_date", "id")
w_end_date_month_by_end_date = Window.partitionBy(F.date_trunc("month", "end_date")).orderBy(F.col("end_date").desc(), "id")
w_all_by_start_date_full = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = (
    df_policies
    .filter(F.col("start_date").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

combined_details = (
    recent.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select("s.*", F.col("p.segment").alias("holder_segment"))
)

with_windows = (
    combined_details
    .select(
        "id",
        "premium",
        "start_date",
        "end_date",
        "status",
        "status_or_unknown",
        "holder_segment",
        F.last("premium", ignorenulls=True).over(w_holder_id_start_date_month_b).alias("last_known_premium"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_segment), F.lit(0)), 4).alias("premium_share_of_holder_segment"),
        F.round(F.percent_rank().over(w_product_holder_id_by_end_dat), 3).alias("end_date_percent_rank"),
        F.sum("premium").over(w_end_date_month_by_end_date_m).alias("premium_sum_4"),
        F.round(F.col("premium") - F.avg("premium").over(w_status_or_unknown), 2).alias("premium_vs_status_or_unknown_avg"),
        F.ntile(5).over(w_all_by_start_date).alias("start_date_quintile"),
        F.row_number().over(w_holder_id_start_date_month_b_2).alias("rn"),
        F.round(F.avg("premium").over(w_holder_segment), 2).alias("holder_segment_avg_premium"),
        F.size(F.collect_set("status").over(w_holder_segment)).alias("distinct_status_in_holder_segment"),
        F.lead("premium", 2).over(w_end_date_month_by_end_date).alias("next2_premium"),
        F.size(F.collect_set("status").over(w_status_or_unknown)).alias("distinct_status_in_status_or_unknown"),
        F.ntile(5).over(w_end_date_month_by_end_date).alias("end_date_quintile"),
        F.row_number().over(w_all_by_start_date).alias("rn_2"),
        F.last("start_date").over(w_all_by_start_date_full).alias("last_start_date"),
    )
)

top_rows = (
    with_windows
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    top_rows
    .filter(F.col("distinct_status_in_status_or_unknown") > 3)
    .select(
        "id",
        "premium",
        "end_date",
        "status",
        "status_or_unknown",
        "holder_segment",
        "last_known_premium",
        "premium_share_of_holder_segment",
        "end_date_percent_rank",
        "premium_sum_4",
        "premium_vs_status_or_unknown_avg",
        "start_date_quintile",
        "next2_premium",
        "end_date_quintile",
        "rn_2",
    )
    .orderBy(F.col("end_date").desc())
)
