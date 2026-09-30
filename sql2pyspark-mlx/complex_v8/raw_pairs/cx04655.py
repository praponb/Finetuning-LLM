from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_valid_to_to_date = Window.partitionBy(F.to_date("valid_to"))
w_all_by_valid_to = Window.orderBy(F.col("valid_to").desc(), "id")
w_all_by_rate_full = Window.orderBy(F.col("rate").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_valid_from_year_by_rate_dist = Window.partitionBy(F.year("valid_from")).orderBy(F.col("rate_distance").desc(), "id")
w_all_by_rate = Window.orderBy(F.col("rate").desc(), "id")
w_all_by_rate_distance_moving = Window.orderBy(F.col("rate_distance").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_valid_from_year_by_rate_dist_2 = Window.partitionBy(F.year("valid_from")).orderBy(F.col("rate_distance").desc())
w_all_by_rate_distance_running = Window.orderBy(F.col("rate_distance").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_valid_to_prior_2 = Window.orderBy(F.col("valid_to").desc(), "id").rowsBetween(-2, -1)
w_all_by_rate_moving_2 = Window.orderBy(F.col("rate").desc(), "id").rowsBetween(-2, Window.currentRow)
w_valid_from_year_by_rate_dist_3 = Window.partitionBy(F.year("valid_from")).orderBy(F.col("rate_distance").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_tariff_windows
    .filter(F.col("tariff").isin("FLAT", "EV"))
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
        F.abs(F.col("rate") - 5.2).alias("rate_distance"),
    )
)

sequenced = (
    filtered
    .select(
        "id",
        "valid_to",
        "rate",
        "rate_distance",
        F.round(F.col("rate_distance") / F.nullif(F.sum("rate_distance").over(w_valid_to_to_date), F.lit(0)), 4).alias("rate_distance_share_of_valid_to_to_date"),
        F.round(F.percent_rank().over(w_all_by_valid_to), 3).alias("valid_to_percent_rank"),
        F.first("valid_to").over(w_all_by_rate_full).alias("first_valid_to"),
        F.round((F.unix_timestamp("valid_from") - F.unix_timestamp(F.lag("valid_from").over(w_valid_from_year_by_rate_dist))) / 60, 1).alias("minutes_since_prev_valid_from"),
        F.row_number().over(w_valid_from_year_by_rate_dist).alias("rn"),
        F.nth_value("rate_distance", 2).over(w_all_by_rate_full).alias("second_rate_distance"),
        F.round(F.percent_rank().over(w_all_by_rate), 3).alias("rate_percent_rank"),
        F.max("rate").over(w_all_by_rate_distance_moving).alias("rate_max_4"),
        F.max("rate_distance").over(w_valid_to_to_date).alias("valid_to_to_date_max_rate_distance"),
        F.nth_value("valid_to", 2).over(w_all_by_rate_full).alias("second_valid_to"),
        F.rank().over(w_valid_from_year_by_rate_dist_2).alias("rate_distance_rank"),
        F.count(F.lit(1)).over(w_all_by_rate_distance_running).alias("running_count"),
        F.max("rate_distance").over(w_all_by_valid_to_prior_2).alias("rate_distance_max_2_prior"),
        F.sum("rate").over(w_all_by_rate_moving_2).alias("rate_sum_3"),
        F.round(F.col("rate_distance") - F.avg("rate_distance").over(w_valid_to_to_date), 2).alias("rate_distance_vs_valid_to_to_date_avg"),
        F.first("valid_to", ignorenulls=True).over(w_valid_from_year_by_rate_dist_3).alias("first_known_valid_to"),
    )
)

top_per_group = (
    sequenced
    .filter(F.col("rn") <= 5)
    .drop("rn")
)

result = (
    top_per_group
    .select(
        "id",
        "valid_to",
        "rate",
        "valid_to_percent_rank",
        "first_valid_to",
        "minutes_since_prev_valid_from",
        "second_rate_distance",
        "rate_percent_rank",
        "rate_max_4",
        "second_valid_to",
        "running_count",
        "rate_sum_3",
        "first_known_valid_to",
    )
    .orderBy(F.col("second_valid_to").desc())
)
