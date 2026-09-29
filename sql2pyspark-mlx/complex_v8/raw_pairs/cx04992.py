from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_start_date_month = Window.partitionBy("holder_id", F.month("start_date"))
w_holder_id = Window.partitionBy("holder_id")
w_status_by_end_date_full = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_by_end_date_prior_3 = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(-3, -1)
w_status_by_end_date_running = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_by_end_date = Window.partitionBy("status").orderBy("end_date", "id")
w_all_by_end_date = Window.orderBy(F.col("end_date").desc(), "id")
w_status_holder_id_by_premium = Window.partitionBy("status", "holder_id").orderBy(F.col("premium").desc(), "id")
w_status_by_end_date_prior_1 = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(-1, -1)

scoped = (
    df_policies
    .filter(F.col("end_date") >= F.lit("2026-02-16").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
    )
)

with_trends = (
    scoped
    .select(
        "*",
        F.count(F.lit(1)).over(w_holder_id_start_date_month).alias("rows_in_holder_id_start_date_month"),
        F.size(F.collect_set("product").over(w_holder_id)).alias("distinct_product_in_holder_id"),
        F.nth_value("start_date", 2).over(w_status_by_end_date_full).alias("second_start_date"),
        F.sum("premium").over(w_status_by_end_date_prior_3).alias("premium_sum_3_prior"),
        F.sum("premium").over(w_status_by_end_date_running).alias("running_premium"),
        F.round(F.col("premium") - F.avg("premium").over(w_holder_id), 2).alias("premium_vs_holder_id_avg"),
        F.row_number().over(w_status_by_end_date).alias("end_date_seq"),
        F.lag("end_date").over(w_all_by_end_date).alias("prev_end_date"),
        F.lead("start_date", 2).over(w_status_holder_id_by_premium).alias("next2_start_date"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_id_start_date_month), F.lit(0)), 4).alias("premium_share_of_holder_id_start_date_month"),
        F.min("premium").over(w_holder_id).alias("holder_id_min_premium"),
        F.round(F.avg("premium").over(w_status_by_end_date_prior_1), 2).alias("premium_avg_1_prior"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "rows_in_holder_id_start_date_month",
        "distinct_product_in_holder_id",
        "premium_sum_3_prior",
        "running_premium",
        "premium_vs_holder_id_avg",
        "end_date_seq",
        "premium_share_of_holder_id_start_date_month",
        "premium_avg_1_prior",
        F.concat_ws(": ", F.col("status"), F.col("product")).alias("status_product_label"),
        F.when(F.col("premium_sum_3_prior") >= 9820.0, "A").when(F.col("premium_sum_3_prior") >= 4640.0, "B").otherwise("C").alias("premium_sum_3_prior_tier"),
    )
    .orderBy(
        F.col("holder_id").desc(),
        F.col("rows_in_holder_id_start_date_month").desc(),
        "id",
    )
    .limit(10)
)
