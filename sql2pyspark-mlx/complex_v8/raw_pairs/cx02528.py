from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_signup_date = Window.orderBy("signup_date", "id")
w_signup_date_month_by_birth_y = Window.partitionBy(F.date_trunc("month", "signup_date")).orderBy("birth_year", "id")
w_signup_date_month_by_birth_y_2 = Window.partitionBy(F.date_trunc("month", "signup_date")).orderBy("birth_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_signup_date_month_by_birth_y_3 = Window.partitionBy(F.date_trunc("month", "signup_date"))
w_signup_date_month_by_birth_y_4 = Window.partitionBy(F.date_trunc("month", "signup_date")).orderBy("birth_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_signup_date_moving_2 = Window.orderBy("signup_date", "id").rowsBetween(-2, Window.currentRow)
w_all_by_signup_date_full = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_seq_no_signup_date_seq = Window.partitionBy("seq_no", "signup_date_seq")
w_seq_no_birth_year_decile = Window.partitionBy("seq_no", "birth_year_decile")
w_signup_date_month_seq_no_by = Window.partitionBy(F.month("signup_date"), "seq_no").orderBy("birth_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_birth_year_decile_signup_dat = Window.partitionBy("birth_year_decile", "signup_date_seq").orderBy(F.col("signup_date_seq").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_first_known_birth_yea = Window.orderBy("first_known_birth_year", "id")
w_birth_year_decile_signup_dat_2 = Window.partitionBy("birth_year_decile", "signup_date_seq").orderBy(F.col("signup_date_seq").desc())

valid_listeners = (
    df_listeners
    .filter((F.col("signup_date") >= F.lit("2024-10-17").cast("date")) & (F.col("plan") == "family"))
    .select(
        "id",
        "signup_date",
        "birth_year",
        "interests",
    )
)

with_windows = (
    valid_listeners
    .select(
        "id",
        "signup_date",
        "birth_year",
        "interests",
        F.row_number().over(w_all_by_signup_date).alias("signup_date_seq"),
        F.row_number().over(w_signup_date_month_by_birth_y).alias("seq_no"),
        F.last("birth_year").over(w_signup_date_month_by_birth_y_2).alias("last_birth_year"),
        F.round(F.col("birth_year") / F.nullif(F.sum("birth_year").over(w_signup_date_month_by_birth_y_3), F.lit(0)), 4).alias("birth_year_share_of_signup_date_month"),
        F.first("birth_year", ignorenulls=True).over(w_signup_date_month_by_birth_y_4).alias("first_known_birth_year"),
        F.sum("birth_year").over(w_all_by_signup_date_moving_2).alias("birth_year_sum_3"),
        F.ntile(10).over(w_signup_date_month_by_birth_y).alias("birth_year_decile"),
        F.count(F.lit(1)).over(w_signup_date_month_by_birth_y_3).alias("rows_in_signup_date_month"),
        F.last("birth_year").over(w_all_by_signup_date_full).alias("last_birth_year_2"),
    )
)

with_trends = (
    with_windows
    .select(
        "*",
        F.round(F.avg("first_known_birth_year").over(w_seq_no_signup_date_seq), 2).alias("seq_no_signup_date_seq_avg_first_known_birth_yea"),
        F.nth_value("signup_date", 2).over(w_signup_date_month_seq_no_by).alias("second_signup_date"),
        F.nth_value("last_birth_year", 2).over(w_birth_year_decile_signup_dat).alias("second_last_birth_year"),
        F.round(F.col("birth_year") / F.nullif(F.sum("birth_year").over(w_seq_no_birth_year_decile), F.lit(0)), 4).alias("birth_year_share_of_seq_no_birth_year_decile"),
        F.row_number().over(w_all_by_first_known_birth_yea).alias("seq_no_2"),
        F.round(F.percent_rank().over(w_birth_year_decile_signup_dat_2), 3).alias("signup_date_seq_percent_rank"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "signup_date",
        "interests",
        "signup_date_seq",
        "last_birth_year",
        "birth_year_share_of_signup_date_month",
        "first_known_birth_year",
        "birth_year_sum_3",
        "birth_year_decile",
        "rows_in_signup_date_month",
        "last_birth_year_2",
        "second_signup_date",
        "second_last_birth_year",
        "birth_year_share_of_seq_no_birth_year_decile",
        "seq_no_2",
        "signup_date_seq_percent_rank",
    )
    .orderBy(F.col("last_birth_year").desc(), "id")
    .limit(10)
)
