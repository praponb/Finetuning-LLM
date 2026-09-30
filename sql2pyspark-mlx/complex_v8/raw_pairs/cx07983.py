from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_product_len = Window.orderBy(F.col("product_len").desc(), "id")
w_end_date_month_status_len_by = Window.partitionBy(F.month("end_date"), "status_len").orderBy("status_len", "id").rowsBetween(-3, Window.currentRow)
w_status_len_holder_id = Window.partitionBy("status_len", "holder_id")
w_all_by_product_len_ties = Window.orderBy(F.col("product_len").desc())
w_all_by_product_len_running = Window.orderBy("product_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_start_date_prior_6 = Window.orderBy(F.col("start_date").asc_nulls_last(), "id").rowsBetween(-6, -1)
w_end_date_month_status_len_by_2 = Window.partitionBy(F.month("end_date"), "status_len").orderBy("status_len")
w_all_by_start_date_full = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_start_date = Window.orderBy("start_date", "id")
w_end_date_month_status_len_by_3 = Window.partitionBy(F.month("end_date"), "status_len").orderBy("status_len", "id")
w_start_date_month_status_len = Window.partitionBy(F.date_trunc("month", "start_date"), "status_len").orderBy("product_len", "id")
w_all_by_start_date_2 = Window.orderBy(F.col("start_date").asc_nulls_last(), "id")
w_status_by_rn_moving_1 = Window.partitionBy("status").orderBy(F.col("rn").desc(), "id").rowsBetween(-1, Window.currentRow)
w_status_by_rn_full = Window.partitionBy("status").orderBy(F.col("rn").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_start_date_quintile = Window.partitionBy("start_date_quintile")

cleaned_policys = (
    df_policies
    .filter((F.col("holder_id") >= 17) & (F.col("status") != "EXPIRED"))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.length(F.trim(F.col("product"))).alias("product_len"),
    )
)

metrics = (
    cleaned_policys
    .select(
        "*",
        F.row_number().over(w_all_by_product_len).alias("seq_no"),
        F.round(F.avg("product_len").over(w_end_date_month_status_len_by), 2).alias("product_len_avg_4"),
        F.size(F.collect_set("status").over(w_status_len_holder_id)).alias("distinct_status_in_status_len_holder_id"),
        F.round(F.percent_rank().over(w_all_by_product_len_ties), 3).alias("product_len_percent_rank"),
        F.min("product_len").over(w_all_by_product_len_running).alias("min_product_len_so_far"),
        F.ntile(10).over(w_all_by_product_len).alias("product_len_decile"),
        F.round(F.avg("product_len").over(w_all_by_start_date_prior_6), 2).alias("product_len_avg_6_prior"),
        F.round(F.cume_dist().over(w_end_date_month_status_len_by_2), 3).alias("status_len_cume_dist"),
        F.nth_value("premium", 2).over(w_all_by_start_date_full).alias("second_premium"),
        F.ntile(5).over(w_all_by_start_date).alias("start_date_quintile"),
        F.row_number().over(w_end_date_month_status_len_by_3).alias("status_len_seq"),
        F.row_number().over(w_start_date_month_status_len).alias("rn"),
        (F.col("product_len") - F.lag("product_len").over(w_all_by_start_date_2)).alias("product_len_change"),
    )
)

windowed = (
    metrics
    .select(
        "*",
        F.max("product_len_change").over(w_status_by_rn_moving_1).alias("product_len_change_max_2"),
        F.last("seq_no").over(w_status_by_rn_full).alias("last_seq_no"),
        F.size(F.collect_set("holder_id").over(w_start_date_quintile)).alias("distinct_holder_id_in_start_date_quintile"),
    )
)

result = (
    windowed
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        "status_len",
        "product_len",
        "product_len_avg_4",
        "distinct_status_in_status_len_holder_id",
        "min_product_len_so_far",
        "product_len_decile",
        "product_len_avg_6_prior",
        "status_len_cume_dist",
        "second_premium",
        "status_len_seq",
        "rn",
        "product_len_change",
        "product_len_change_max_2",
        "last_seq_no",
        "distinct_holder_id_in_start_date_quintile",
        (F.col("last_seq_no") + F.col("seq_no")).alias("last_seq_no_plus_seq_no"),
        F.month(F.col("start_date")).alias("start_date_month"),
    )
)
