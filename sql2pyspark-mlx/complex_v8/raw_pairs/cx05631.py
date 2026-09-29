from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_valid_from_month_valid_to_to = Window.partitionBy(F.month("valid_from"), F.to_date("valid_to")).orderBy(F.col("valid_from").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_tariff_valid_to_to_date_by_v = Window.partitionBy("tariff", F.to_date("valid_to")).orderBy("valid_from", "id")
w_row_count_valid_from_day_mon = Window.partitionBy("row_count", F.month("valid_from_day"))
w_n_tariff_tou_valid_from_day = Window.partitionBy("n_tariff_tou", F.year("valid_from_day")).orderBy("n_tariff_tou", "valid_from_day")
w_n_tariff_tou_valid_from_day_2 = Window.partitionBy("n_tariff_tou", F.year("valid_from_day")).orderBy("n_tariff_tou", "valid_from_day").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_n_tariff_tou_valid_from_day_3 = Window.partitionBy("n_tariff_tou", F.year("valid_from_day")).orderBy("n_tariff_tou", "valid_from_day").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_tariff_windows
    .filter((F.month(F.col("valid_to")).isin(1, 2)) & (F.col("tariff") == "EV"))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
    )
)

with_windows = (
    prepared
    .select(
        "*",
        F.last("valid_from", ignorenulls=True).over(w_valid_from_month_valid_to_to).alias("last_known_valid_from"),
        F.row_number().over(w_tariff_valid_to_to_date_by_v).alias("valid_from_seq"),
        F.lag("valid_from").over(w_tariff_valid_to_to_date_by_v).alias("prev_valid_from"),
    )
)

tariff_stats = (
    with_windows
    .groupBy("tariff")
    .agg(F.avg("valid_from_seq").alias("avg_valid_from_seq"))
)

outliers = (
    with_windows.alias("s")
    .join(tariff_stats.alias("g"), F.col("g.tariff") == F.col("s.tariff"), "inner")
    .filter(F.col("s.valid_from_seq") > F.col("g.avg_valid_from_seq"))
    .select(
        "s.*",
        "g.avg_valid_from_seq",
        F.round(F.col("s.valid_from_seq") - F.col("g.avg_valid_from_seq"), 2).alias("valid_from_seq_minus_avg"),
    )
)

stats = (
    outliers
    .groupBy(F.to_date("valid_from").alias("valid_from_day"))
    .agg(
        F.count(F.when(F.col("tariff") == "TOU", 1)).alias("n_tariff_tou"),
        F.max("valid_from_seq_minus_avg").alias("max_valid_from_seq_minus_avg"),
        F.count("*").alias("row_count"),
        F.round(F.sum("valid_from_seq_minus_avg") / F.nullif(F.sum("avg_valid_from_seq"), F.lit(0)), 3).alias("valid_from_seq_minus_avg_to_avg_valid_from_seq_r"),
    )
    .filter(F.col("row_count") >= 2)
)

without_matches = (
    stats
    .crossJoin(stats.agg(F.avg("row_count").alias("overall_avg_row_count")))
    .filter(F.col("row_count") > F.col("overall_avg_row_count"))
    .drop("overall_avg_row_count")
)

windowed = (
    without_matches
    .select(
        "*",
        F.sum("row_count").over(w_row_count_valid_from_day_mon).alias("row_count_valid_from_day_month_sum_row_count"),
        F.datediff(F.col("valid_from_day"), F.lag("valid_from_day").over(w_n_tariff_tou_valid_from_day)).alias("days_since_prev_valid_from_day"),
        F.first("max_valid_from_seq_minus_avg", ignorenulls=True).over(w_n_tariff_tou_valid_from_day_2).alias("first_known_max_valid_from_seq_minus_avg"),
        F.count(F.lit(1)).over(w_row_count_valid_from_day_mon).alias("rows_in_row_count_valid_from_day_month"),
        F.nth_value("n_tariff_tou", 2).over(w_n_tariff_tou_valid_from_day_3).alias("second_n_tariff_tou"),
    )
)

result = (
    windowed
    .select(
        "valid_from_day",
        "n_tariff_tou",
        "row_count",
        "valid_from_seq_minus_avg_to_avg_valid_from_seq_r",
        "row_count_valid_from_day_month_sum_row_count",
        "days_since_prev_valid_from_day",
        "first_known_max_valid_from_seq_minus_avg",
        "rows_in_row_count_valid_from_day_month",
        F.abs(F.col("second_n_tariff_tou") - 6).alias("second_n_tariff_tou_distance"),
    )
    .orderBy(F.col("first_known_max_valid_from_seq_minus_avg").desc())
)
