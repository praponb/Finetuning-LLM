from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_by_valid_from_full = Window.partitionBy("tariff").orderBy(F.col("valid_from").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_tariff = Window.partitionBy("tariff")
w_valid_from_year_tariff_by_mi = Window.partitionBy(F.year("valid_from"), "tariff").orderBy(F.col("minutes_valid_to_to_valid_from").desc())
w_all_by_rate = Window.orderBy(F.col("rate").desc(), "id")
w_valid_from_year_tariff_by_mi_2 = Window.partitionBy(F.year("valid_from"), "tariff").orderBy(F.col("minutes_valid_to_to_valid_from").desc(), "id")
w_valid_to_month_tariff = Window.partitionBy(F.month("valid_to"), "tariff")
w_all_by_valid_to_running = Window.orderBy("valid_to", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_tariff_by_running_rate = Window.partitionBy("tariff").orderBy(F.col("running_rate").asc_nulls_last(), "id")
w_second_valid_to_month_valid = Window.partitionBy(F.date_trunc("month", "second_valid_to"), F.date_trunc("month", "valid_to")).orderBy(F.col("rows_in_valid_to_month_tariff").desc(), "id").rowsBetween(-3, -1)

scoped_tariffs = (
    df_tariff_windows
    .filter((F.col("rate") >= 3.0) & (F.col("tariff") != "TOU"))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.round((F.unix_timestamp(F.col("valid_from")) - F.unix_timestamp(F.col("valid_to"))) / 60, 1).alias("minutes_valid_to_to_valid_from"),
    )
)

windowed = (
    scoped_tariffs
    .select(
        "*",
        F.nth_value("valid_to", 2).over(w_tariff_by_valid_from_full).alias("second_valid_to"),
        F.round(F.col("rate") / F.nullif(F.sum("rate").over(w_tariff), F.lit(0)), 4).alias("rate_share_of_tariff"),
        F.dense_rank().over(w_valid_from_year_tariff_by_mi).alias("minutes_valid_to_to_valid_from_dense_rank"),
        F.lag("valid_to").over(w_all_by_rate).alias("prev_valid_to"),
        F.row_number().over(w_valid_from_year_tariff_by_mi_2).alias("minutes_valid_to_to_valid_from_seq"),
        F.count(F.lit(1)).over(w_valid_to_month_tariff).alias("rows_in_valid_to_month_tariff"),
        F.sum("rate").over(w_all_by_valid_to_running).alias("running_rate"),
        F.round(F.avg("rate").over(w_valid_to_month_tariff), 2).alias("valid_to_month_tariff_avg_rate"),
        F.round(F.col("rate") - F.avg("rate").over(w_tariff), 2).alias("rate_vs_tariff_avg"),
    )
)

metrics = (
    windowed
    .select(
        "*",
        F.round(F.col("rate_vs_tariff_avg") - F.avg("rate_vs_tariff_avg").over(w_tariff), 2).alias("rate_vs_tariff_avg_vs_tariff_avg"),
        F.lead("prev_valid_to").over(w_tariff_by_running_rate).alias("next_prev_valid_to"),
        F.max("rate_vs_tariff_avg").over(w_second_valid_to_month_valid).alias("rate_vs_tariff_avg_max_3_prior"),
        F.round(F.col("rate") / F.nullif(F.sum("rate").over(w_tariff), F.lit(0)), 4).alias("rate_share_of_tariff_2"),
        F.round(F.col("rate_share_of_tariff") - F.avg("rate_share_of_tariff").over(w_tariff), 2).alias("rate_share_of_tariff_vs_tariff_avg"),
        F.round(F.avg("rate").over(w_tariff), 2).alias("tariff_avg_rate"),
    )
)

result = (
    metrics
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "second_valid_to",
        "minutes_valid_to_to_valid_from_dense_rank",
        "prev_valid_to",
        "rows_in_valid_to_month_tariff",
        "running_rate",
        "valid_to_month_tariff_avg_rate",
        "rate_vs_tariff_avg",
        "rate_vs_tariff_avg_vs_tariff_avg",
        "next_prev_valid_to",
        "rate_share_of_tariff_2",
        "rate_share_of_tariff_vs_tariff_avg",
        "tariff_avg_rate",
    )
)
