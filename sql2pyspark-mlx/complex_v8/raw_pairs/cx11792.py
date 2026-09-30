from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_status = Window.partitionBy("holder_id", "status")
w_all_by_premium = Window.orderBy("premium", "id")
w_all_by_start_date_full = Window.orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_premium_moving_2 = Window.orderBy("premium", "id").rowsBetween(-2, Window.currentRow)
w_all_by_start_date = Window.orderBy(F.col("start_date").desc(), "id")
w_all_by_start_date_running = Window.orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_status_by_premium = Window.partitionBy("holder_id", "status").orderBy("premium", "id")
w_holder_id_status_by_start_da = Window.partitionBy("holder_id", "status").orderBy("start_date", "id")
w_all_by_premium_ties = Window.orderBy("premium")
w_all_by_start_date_2 = Window.orderBy("start_date", "id")
w_all_by_premium_full = Window.orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_policies
    .filter((F.col("holder_id") >= 8) & (F.col("start_date") >= F.lit("2024-10-22").cast("date")))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
    )
)

ranked = (
    valid
    .select(
        "*",
        F.max("premium").over(w_holder_id_status).alias("holder_id_status_max_premium"),
        F.row_number().over(w_all_by_premium).alias("premium_seq"),
        F.nth_value("start_date", 2).over(w_all_by_start_date_full).alias("second_start_date"),
        F.round(F.avg("premium").over(w_holder_id_status), 2).alias("holder_id_status_avg_premium"),
        F.round(F.avg("premium").over(w_all_by_premium_moving_2), 2).alias("premium_avg_3"),
        F.lead("start_date").over(w_all_by_start_date).alias("next_start_date"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_id_status), F.lit(0)), 4).alias("premium_share_of_holder_id_status"),
        F.max("premium").over(w_all_by_start_date_running).alias("max_premium_so_far"),
        F.sum("premium").over(w_holder_id_status).alias("holder_id_status_sum_premium"),
        F.row_number().over(w_holder_id_status_by_premium).alias("premium_seq_2"),
        F.row_number().over(w_holder_id_status_by_start_da).alias("seq_no"),
        F.dense_rank().over(w_all_by_premium_ties).alias("premium_dense_rank"),
        F.round(F.percent_rank().over(w_all_by_premium), 3).alias("premium_percent_rank"),
        F.round(F.cume_dist().over(w_all_by_start_date_2), 3).alias("start_date_cume_dist"),
        F.last("premium").over(w_all_by_premium_full).alias("last_premium"),
    )
)

first_rows = (
    ranked
    .filter(F.col("premium_seq") == 1)
    .drop("premium_seq")
)

result = (
    first_rows
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
        "holder_id_status_max_premium",
        "second_start_date",
        "holder_id_status_avg_premium",
        "premium_avg_3",
        "premium_share_of_holder_id_status",
        "max_premium_so_far",
        "premium_seq_2",
        "seq_no",
        "start_date_cume_dist",
        "last_premium",
        (F.floor(F.col("premium_dense_rank") / 2) * 2).alias("premium_dense_rank_bucket"),
        F.substring(F.trim(F.col("status")), 1, 3).alias("status_prefix"),
    )
    .orderBy(F.col("max_premium_so_far").desc_nulls_first())
)
