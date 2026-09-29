from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_start_date_month_e = Window.partitionBy("status").orderBy(F.col("start_date_month_end").desc())
w_start_date_month_by_start_da = Window.partitionBy(F.date_trunc("month", "start_date")).orderBy(F.col("start_date_month_end").desc())
w_holder_id_status_by_start_da = Window.partitionBy("holder_id", "status").orderBy("start_date_year", "id")
w_holder_id_start_date_month_b = Window.partitionBy("holder_id", F.date_trunc("month", "start_date")).orderBy("start_date_year", "id")
w_holder_id_status_by_start_da_2 = Window.partitionBy("holder_id", "status").orderBy("start_date_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_start_date_month_by_start_da_2 = Window.partitionBy(F.date_trunc("month", "start_date")).orderBy(F.col("start_date_month_end").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_start_date_month_b_2 = Window.partitionBy("holder_id", F.date_trunc("month", "start_date")).orderBy("start_date_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_by_start_date_month_e_2 = Window.partitionBy("status").orderBy(F.col("start_date_month_end").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_by_start_date_month_e_3 = Window.partitionBy("status").orderBy(F.col("start_date_month_end").desc(), "id")
w_holder_id = Window.partitionBy("holder_id")
w_start_date_year_decile_start = Window.partitionBy("start_date_year_decile", F.to_date("start_date_year")).orderBy("start_date_month_end_percent_rank_2", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_last_start_date_runni = Window.orderBy("last_start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_last_start_date_full = Window.orderBy("last_start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_start_date_month_end = Window.orderBy("start_date_month_end_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_policies
    .filter((F.col("status").isin("CANCELLED", "ACTIVE")) & (F.year(F.col("end_date")) == 2026))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
        F.date_trunc("year", F.col("start_date")).alias("start_date_year"),
        F.last_day(F.col("start_date")).alias("start_date_month_end"),
    )
)

with_trends = (
    recent
    .select(
        "id",
        "holder_id",
        "start_date",
        "status",
        "start_date_year",
        "start_date_month_end",
        F.round(F.percent_rank().over(w_status_by_start_date_month_e), 3).alias("start_date_month_end_percent_rank"),
        F.dense_rank().over(w_status_by_start_date_month_e).alias("start_date_month_end_dense_rank"),
        F.round(F.percent_rank().over(w_start_date_month_by_start_da), 3).alias("start_date_month_end_percent_rank_2"),
        F.lag("start_date").over(w_holder_id_status_by_start_da).alias("prev_start_date"),
        F.row_number().over(w_holder_id_start_date_month_b).alias("start_date_year_seq"),
        F.ntile(10).over(w_holder_id_start_date_month_b).alias("start_date_year_decile"),
        F.first("start_date").over(w_holder_id_status_by_start_da_2).alias("first_start_date"),
        F.last("start_date").over(w_start_date_month_by_start_da_2).alias("last_start_date"),
        F.last("start_date", ignorenulls=True).over(w_holder_id_start_date_month_b_2).alias("last_known_start_date"),
        F.nth_value("start_date_year", 2).over(w_status_by_start_date_month_e_2).alias("second_start_date_year"),
        F.last("start_date_month_end").over(w_start_date_month_by_start_da_2).alias("last_start_date_month_end"),
        F.lag("start_date_month_end").over(w_status_by_start_date_month_e_3).alias("prev_start_date_month_end"),
        F.ntile(3).over(w_status_by_start_date_month_e_3).alias("start_date_month_end_tercile"),
    )
)

with_windows = (
    with_trends
    .select(
        "*",
        F.max("start_date_year_seq").over(w_holder_id).alias("holder_id_max_start_date_year_seq"),
        F.last("prev_start_date_month_end").over(w_start_date_year_decile_start).alias("last_prev_start_date_month_end"),
        F.round(F.col("start_date_year_decile") - F.avg("start_date_year_decile").over(w_holder_id), 2).alias("start_date_year_decile_vs_holder_id_avg"),
        F.first("start_date_month_end", ignorenulls=True).over(w_all_by_last_start_date_runni).alias("first_known_start_date_month_end"),
        F.first("start_date_month_end").over(w_all_by_last_start_date_full).alias("first_start_date_month_end"),
        F.max("start_date_month_end_tercile").over(w_all_by_start_date_month_end).alias("max_start_date_month_end_tercile_so_far"),
    )
)

result = (
    with_windows
    .filter(F.col("start_date_year_seq") < 6)
    .select(
        "id",
        "holder_id",
        "start_date_year",
        "start_date_month_end",
        "start_date_month_end_dense_rank",
        "start_date_month_end_percent_rank_2",
        "start_date_year_seq",
        "start_date_year_decile",
        "first_start_date",
        "last_start_date",
        "last_known_start_date",
        "second_start_date_year",
        "last_start_date_month_end",
        "prev_start_date_month_end",
        "start_date_month_end_tercile",
        "holder_id_max_start_date_year_seq",
        "last_prev_start_date_month_end",
        "first_known_start_date_month_end",
        "first_start_date_month_end",
        "max_start_date_month_end_tercile_so_far",
        F.coalesce(F.col("start_date_year_decile"), F.lit(0)).alias("start_date_year_decile_filled"),
        F.round((F.unix_timestamp(F.col("start_date_year")) - F.unix_timestamp(F.col("second_start_date_year"))) / 60, 1).alias("minutes_second_start_date_year_to_start_date_yea"),
    )
    .orderBy(
        F.col("start_date_year").desc_nulls_first(),
        F.col("start_date_month_end_tercile").desc(),
    )
)
