from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_status_by_end_date_f = Window.partitionBy("product", "status").orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_status = Window.partitionBy("holder_id", "status")
w_status_start_date_month_by_s = Window.partitionBy("status", F.month("start_date")).orderBy(F.col("start_date").desc(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_start_date_ties = Window.orderBy("start_date")
w_all_by_start_date = Window.orderBy("start_date", "id")
w_all_by_start_date_prior_3 = Window.orderBy("start_date", "id").rowsBetween(-3, -1)
w_status = Window.partitionBy("status")
w_status_holder_id_by_start_da = Window.partitionBy("status", "holder_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_end_date_year = Window.partitionBy("holder_id", F.year("end_date"))
w_status_holder_id_by_start_da_2 = Window.partitionBy("status", "holder_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_start_date_month_by_s_2 = Window.partitionBy("status", F.month("start_date")).orderBy(F.col("start_date").desc(), "id")
w_product_start_date_month_by = Window.partitionBy("product", F.date_trunc("month", "start_date")).orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_status_by_premium = Window.partitionBy("holder_id", "status").orderBy(F.col("premium").asc_nulls_last(), "id")
w_prev2_start_date_year_by_las = Window.partitionBy(F.year("prev2_start_date")).orderBy(F.col("last_start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_premium_sum_3_prior_t = Window.orderBy(F.col("premium_sum_3_prior").desc())
w_all_by_start_date_percent_ra = Window.orderBy("start_date_percent_rank")
w_all_by_premium_sum_3_prior = Window.orderBy(F.col("premium_sum_3_prior").desc(), "id")

recent = (
    df_policies
    .filter(F.col("end_date") < F.lit("2026-10-23").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.initcap(F.trim(F.col("status"))).alias("status_clean"),
    )
)

with_windows = (
    recent
    .select(
        "*",
        F.last("end_date").over(w_product_status_by_end_date_f).alias("last_end_date"),
        F.max("premium").over(w_holder_id_status).alias("holder_id_status_max_premium"),
        F.max("premium").over(w_status_start_date_month_by_s).alias("premium_max_3"),
        F.round(F.percent_rank().over(w_all_by_start_date_ties), 3).alias("start_date_percent_rank"),
        F.rank().over(w_all_by_start_date).alias("start_date_rank"),
        F.round(F.avg("premium").over(w_all_by_start_date_prior_3), 2).alias("premium_avg_3_prior"),
        F.round(F.col("premium") - F.avg("premium").over(w_status), 2).alias("premium_vs_status_avg"),
        F.first("start_date").over(w_status_holder_id_by_start_da).alias("first_start_date"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_id_end_date_year), F.lit(0)), 4).alias("premium_share_of_holder_id_end_date_year"),
        F.sum("premium").over(w_all_by_start_date_prior_3).alias("premium_sum_3_prior"),
        F.min("premium").over(w_status_holder_id_by_start_da_2).alias("min_premium_so_far"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_status_start_date_month_by_s_2)).alias("days_since_prev_start_date"),
        F.last("start_date").over(w_product_start_date_month_by).alias("last_start_date"),
        F.lag("start_date", 2).over(w_holder_id_status_by_premium).alias("prev2_start_date"),
    )
)

top_rows = (
    with_windows
    .filter(F.col("start_date_rank") <= 2)
    .drop("start_date_rank")
)

ranked = (
    top_rows
    .select(
        "*",
        F.max("start_date_percent_rank").over(w_prev2_start_date_year_by_las).alias("max_start_date_percent_rank_so_far"),
        F.rank().over(w_all_by_premium_sum_3_prior_t).alias("premium_sum_3_prior_rank"),
        F.dense_rank().over(w_all_by_start_date_percent_ra).alias("start_date_percent_rank_dense_rank"),
        F.row_number().over(w_all_by_premium_sum_3_prior).alias("seq_no"),
    )
)

result = (
    ranked
    .filter(F.col("days_since_prev_start_date") >= 80)
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        "last_end_date",
        "holder_id_status_max_premium",
        "premium_max_3",
        "start_date_percent_rank",
        "premium_avg_3_prior",
        "first_start_date",
        "premium_sum_3_prior",
        "min_premium_so_far",
        "days_since_prev_start_date",
        "last_start_date",
        "max_start_date_percent_rank_so_far",
        "premium_sum_3_prior_rank",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
    .orderBy(F.col("holder_id").desc())
)
