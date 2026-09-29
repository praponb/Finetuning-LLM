from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_product = Window.partitionBy("status", "product")
w_product_by_start_date_prior = Window.partitionBy("product").orderBy(F.col("start_date").desc(), "id").rowsBetween(-2, -1)
w_all_by_end_date = Window.orderBy(F.col("end_date").desc(), "id")
w_status_end_date_month_by_sta = Window.partitionBy("status", F.month("end_date")).orderBy("start_date", "id")
w_all_by_end_date_ties = Window.orderBy(F.col("end_date").desc())
w_status_end_date_month_by_sta_2 = Window.partitionBy("status", F.month("end_date")).orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_product_by_start_date = Window.partitionBy("product").orderBy(F.col("start_date").desc(), "id")
w_start_date_month_holder_id = Window.partitionBy(F.month("start_date"), "holder_id")

scoped = (
    df_policies
    .filter(F.col("holder_id") <= 15)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

windowed = (
    scoped
    .select(
        "*",
        F.round(F.col("premium") - F.avg("premium").over(w_status_product), 2).alias("premium_vs_status_product_avg"),
        F.max("premium").over(w_product_by_start_date_prior).alias("premium_max_2_prior"),
        F.row_number().over(w_all_by_end_date).alias("end_date_seq"),
        F.row_number().over(w_status_end_date_month_by_sta).alias("start_date_seq"),
        F.rank().over(w_all_by_end_date_ties).alias("end_date_rank"),
        F.rank().over(w_all_by_end_date).alias("end_date_rank_2"),
        F.nth_value("start_date", 2).over(w_status_end_date_month_by_sta_2).alias("second_start_date"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_product_by_start_date)).alias("days_since_prev_start_date"),
        F.count(F.lit(1)).over(w_start_date_month_holder_id).alias("rows_in_start_date_month_holder_id"),
        F.dense_rank().over(w_product_by_start_date).alias("start_date_dense_rank"),
    )
)

scored = (
    windowed
    .filter(F.col("premium_max_2_prior") > 4640.0)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        "premium_vs_status_product_avg",
        "premium_max_2_prior",
        "end_date_rank_2",
        "second_start_date",
        "rows_in_start_date_month_holder_id",
        "start_date_dense_rank",
        F.lower(F.trim(F.col("product"))).alias("product_clean"),
    )
)

result = (
    scored
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
        "premium_vs_status_product_avg",
        "premium_max_2_prior",
        "end_date_rank_2",
        "rows_in_start_date_month_holder_id",
        "product_clean",
    )
    .orderBy(F.col("product").desc())
)
