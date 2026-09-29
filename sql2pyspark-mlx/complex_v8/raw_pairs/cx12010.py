from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_holder_id_by_end_date = Window.partitionBy("status", "holder_id").orderBy(F.col("end_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_end_date_month_holder_id = Window.partitionBy(F.date_trunc("month", "end_date"), "holder_id")
w_product = Window.partitionBy("product")
w_status_by_start_date_running = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_by_start_date_ties = Window.partitionBy("status").orderBy("start_date")
w_status_by_start_date_prior_1 = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(-1, -1)
w_holder_id_by_end_date = Window.partitionBy("holder_id").orderBy("end_date", "id")
w_status_holder_id_by_end_date_2 = Window.partitionBy("status", "holder_id").orderBy(F.col("end_date").asc_nulls_last(), "id")

prepared = (
    df_policies
    .filter(F.col("start_date") >= F.lit("2024-06-19").cast("date"))
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
    prepared
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        F.last("start_date").over(w_status_holder_id_by_end_date).alias("last_start_date"),
        F.max("premium").over(w_end_date_month_holder_id).alias("end_date_month_holder_id_max_premium"),
        F.size(F.collect_set("product").over(w_end_date_month_holder_id)).alias("distinct_product_in_end_date_month_holder_id"),
        F.round(F.col("premium") - F.avg("premium").over(w_product), 2).alias("premium_vs_product_avg"),
        F.size(F.collect_set("status").over(w_end_date_month_holder_id)).alias("distinct_status_in_end_date_month_holder_id"),
        F.first("start_date", ignorenulls=True).over(w_status_by_start_date_running).alias("first_known_start_date"),
        F.max("premium").over(w_product).alias("product_max_premium"),
        F.dense_rank().over(w_status_by_start_date_ties).alias("start_date_dense_rank"),
        F.round(F.avg("premium").over(w_status_by_start_date_prior_1), 2).alias("premium_avg_1_prior"),
        F.count(F.lit(1)).over(w_end_date_month_holder_id).alias("rows_in_end_date_month_holder_id"),
        F.lead("start_date").over(w_holder_id_by_end_date).alias("next_start_date"),
        F.round(F.col("premium") - F.avg("premium").over(w_end_date_month_holder_id), 2).alias("premium_vs_end_date_month_holder_id_avg"),
        F.size(F.collect_set("status").over(w_product)).alias("distinct_status_in_product"),
        F.lead("end_date", 3).over(w_status_holder_id_by_end_date_2).alias("next3_end_date"),
    )
)

leaders = (
    windowed
    .filter(F.col("start_date_dense_rank") <= 2)
    .drop("start_date_dense_rank")
)

result = (
    leaders
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        "premium_vs_product_avg",
        "distinct_status_in_end_date_month_holder_id",
        "first_known_start_date",
        "product_max_premium",
        "premium_avg_1_prior",
        "rows_in_end_date_month_holder_id",
        "next_start_date",
        "premium_vs_end_date_month_holder_id_avg",
        "next3_end_date",
    )
    .orderBy("holder_id")
)
