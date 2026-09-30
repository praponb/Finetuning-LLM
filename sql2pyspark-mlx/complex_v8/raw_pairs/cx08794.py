from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_valid_from_month_by_valid_fr = Window.partitionBy("valid_from_month").orderBy(F.col("valid_from_month").desc(), "id").rowsBetween(-6, -1)
w_all_by_minutes_valid_from_to = Window.orderBy(F.col("minutes_valid_from_to_valid_to").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_valid_from_month_by_valid_fr_2 = Window.partitionBy("valid_from_month").orderBy(F.col("valid_from_month").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_minutes_valid_from_to_2 = Window.orderBy(F.col("minutes_valid_from_to_valid_to").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_valid_to_quarter = Window.partitionBy("valid_to_quarter")
w_second_valid_to_month_valid = Window.partitionBy(F.date_trunc("month", "second_valid_to"), "valid_to_quarter").orderBy("rate_sum_6_prior", "id")

cleaned = (
    df_tariff_windows
    .filter((F.col("valid_to") >= F.lit("2025-01-25 21:00:00").cast("timestamp")) & (F.col("tariff").isin("EV", "FLAT")))
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
        F.month(F.col("valid_from")).alias("valid_from_month"),
    )
)

derived = (
    cleaned
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
        "valid_from_month",
        F.round((F.unix_timestamp(F.col("valid_to")) - F.unix_timestamp(F.col("valid_from"))) / 60, 1).alias("minutes_valid_from_to_valid_to"),
        F.quarter(F.col("valid_to")).alias("valid_to_quarter"),
    )
)

windowed = (
    derived
    .select(
        "id",
        "valid_to",
        "minutes_valid_from_to_valid_to",
        "valid_to_quarter",
        F.sum("rate").over(w_valid_from_month_by_valid_fr).alias("rate_sum_6_prior"),
        F.nth_value("valid_to", 2).over(w_all_by_minutes_valid_from_to).alias("second_valid_to"),
        F.first("rate", ignorenulls=True).over(w_valid_from_month_by_valid_fr_2).alias("first_known_rate"),
        F.max("rate").over(w_all_by_minutes_valid_from_to_2).alias("max_rate_so_far"),
    )
)

metrics = (
    windowed
    .select(
        "*",
        F.sum("valid_to_quarter").over(w_valid_to_quarter).alias("valid_to_quarter_sum_valid_to_quarter"),
        F.rank().over(w_second_valid_to_month_valid).alias("rate_sum_6_prior_rank"),
        F.count(F.lit(1)).over(w_valid_to_quarter).alias("rows_in_valid_to_quarter"),
    )
)

leaders = (
    metrics
    .filter(F.col("rate_sum_6_prior_rank") <= 3)
    .drop("rate_sum_6_prior_rank")
)

result = (
    leaders
    .select(
        "id",
        "valid_to",
        "minutes_valid_from_to_valid_to",
        "valid_to_quarter",
        "second_valid_to",
        "first_known_rate",
        "valid_to_quarter_sum_valid_to_quarter",
        "rows_in_valid_to_quarter",
    )
    .orderBy(F.col("first_known_rate").asc(), F.col("valid_to_quarter").desc())
)
