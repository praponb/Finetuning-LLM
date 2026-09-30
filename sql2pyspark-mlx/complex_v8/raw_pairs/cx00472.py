from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_start_date_ties = Window.orderBy("start_date")
w_holder_id_by_days_end_date_t = Window.partitionBy("holder_id").orderBy(F.col("days_end_date_to_start_date").desc(), "id").rowsBetween(-6, Window.currentRow)
w_product_by_premium = Window.partitionBy("product").orderBy(F.col("premium").desc(), "id")
w_product_status = Window.partitionBy("product", "status")
w_all_by_end_date = Window.orderBy("end_date", "id")
w_all_by_premium_ties = Window.orderBy(F.col("premium").desc_nulls_first())
w_holder_id_by_days_end_date_t_2 = Window.partitionBy("holder_id").orderBy(F.col("days_end_date_to_start_date").desc(), "id")
w_all_by_end_date_moving_2 = Window.orderBy("end_date", "id").rowsBetween(-2, Window.currentRow)
w_start_date_year_start_date_d = Window.partitionBy(F.year("start_date"), "start_date_dense_rank")
w_product_by_start_date_runnin = Window.partitionBy("product").orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_by_start_date = Window.partitionBy("product").orderBy(F.col("start_date").desc(), "id")
w_all_by_start_date_percent_ra = Window.orderBy("start_date_percent_rank")
w_all_by_start_date_percent_ra_2 = Window.orderBy("start_date_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_start_date_dense_rank_by_sta = Window.partitionBy("start_date_dense_rank").orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id = Window.partitionBy("holder_id")
w_start_date_dense_rank_holder = Window.partitionBy("start_date_dense_rank", "holder_id").orderBy("total_start_date_percent_rank", "start_date_dense_rank", "holder_id").rowsBetween(-1, Window.currentRow)

valid_policys = (
    df_policies
    .filter(F.col("start_date") < F.lit("2025-05-28").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.datediff(F.col("start_date"), F.col("end_date")).alias("days_end_date_to_start_date"),
    )
)

metrics = (
    valid_policys
    .select(
        "*",
        F.dense_rank().over(w_all_by_start_date_ties).alias("start_date_dense_rank"),
        F.round(F.avg("premium").over(w_holder_id_by_days_end_date_t), 2).alias("premium_avg_7"),
        F.lead("days_end_date_to_start_date", 3, 0).over(w_product_by_premium).alias("next3_days_end_date_to_start_date"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_product_status), F.lit(0)), 4).alias("premium_share_of_product_status"),
        F.lag("premium", 2).over(w_all_by_end_date).alias("prev2_premium"),
        F.count(F.lit(1)).over(w_product_status).alias("rows_in_product_status"),
        F.round(F.percent_rank().over(w_all_by_start_date_ties), 3).alias("start_date_percent_rank"),
        F.lag("days_end_date_to_start_date", 3).over(w_product_by_premium).alias("prev3_days_end_date_to_start_date"),
        F.lag("premium", 3, 0).over(w_all_by_end_date).alias("prev3_premium"),
        F.round(F.percent_rank().over(w_all_by_premium_ties), 3).alias("premium_percent_rank"),
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_holder_id_by_days_end_date_t_2)).alias("days_since_prev_end_date"),
        F.row_number().over(w_holder_id_by_days_end_date_t_2).alias("days_end_date_to_start_date_seq"),
        F.max("premium").over(w_all_by_end_date_moving_2).alias("premium_max_3"),
        F.round(F.col("premium") - F.avg("premium").over(w_product_status), 2).alias("premium_vs_product_status_avg"),
    )
)

first_rows = (
    metrics
    .filter(F.col("days_end_date_to_start_date_seq") <= 3)
    .drop("days_end_date_to_start_date_seq")
)

sequenced = (
    first_rows
    .select(
        "*",
        F.size(F.collect_set("status").over(w_start_date_year_start_date_d)).alias("distinct_status_in_start_date_year_start_date_de"),
        F.first("rows_in_product_status", ignorenulls=True).over(w_product_by_start_date_runnin).alias("first_known_rows_in_product_status"),
        F.lead("start_date_dense_rank").over(w_product_by_start_date).alias("next_start_date_dense_rank"),
        F.round(F.percent_rank().over(w_all_by_start_date_percent_ra), 3).alias("start_date_percent_rank_percent_rank"),
        F.first("premium_max_3", ignorenulls=True).over(w_all_by_start_date_percent_ra_2).alias("first_known_premium_max_3"),
        F.last("premium_avg_7").over(w_start_date_dense_rank_by_sta).alias("last_premium_avg_7"),
    )
)

grouped = (
    sequenced
    .groupBy("start_date_dense_rank", "holder_id")
    .agg(
        F.round(F.sum("start_date_percent_rank"), 2).alias("total_start_date_percent_rank"),
        F.min("end_date").alias("first_end_date"),
        F.array_sort(F.collect_set("product")).alias("product_list"),
        F.round(100 * F.count(F.when(F.col("status").isNotNull(), 1)) / F.count("*"), 1).alias("pct_status_is_not_null"),
    )
)

metrics_2 = (
    grouped
    .select(
        "start_date_dense_rank",
        "holder_id",
        "first_end_date",
        "product_list",
        "pct_status_is_not_null",
        F.sum("start_date_dense_rank").over(w_holder_id).alias("holder_id_sum_start_date_dense_rank"),
        F.max("start_date_dense_rank").over(w_start_date_dense_rank_holder).alias("start_date_dense_rank_max_2"),
    )
)

result = (
    metrics_2
    .filter(F.col("first_end_date") >= F.lit("2026-02-16").cast("date"))
    .select(
        "start_date_dense_rank",
        "holder_id",
        "first_end_date",
        "product_list",
        "pct_status_is_not_null",
        "start_date_dense_rank_max_2",
    )
    .orderBy(
        "start_date_dense_rank_max_2",
        F.col("first_end_date").asc(),
        "start_date_dense_rank",
        "holder_id",
    )
    .limit(5)
)
