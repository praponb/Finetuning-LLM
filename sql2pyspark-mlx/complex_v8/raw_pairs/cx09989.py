from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_by_rate = Window.partitionBy("tariff").orderBy(F.col("rate").desc(), "id")
w_tariff = Window.partitionBy("tariff")
w_valid_from_to_date_tariff_by = Window.partitionBy(F.to_date("valid_from"), "tariff").orderBy(F.col("valid_from").desc(), "id")
w_valid_from_to_date_tariff_by_2 = Window.partitionBy(F.to_date("valid_from"), "tariff").orderBy(F.col("valid_from").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_tariff_valid_from_dense_rank = Window.partitionBy("tariff", "valid_from_dense_rank")
w_first_valid_to_month_by_firs = Window.partitionBy(F.month("first_valid_to")).orderBy("first_valid_to", "id")
w_tariff_first_valid_to_year_b = Window.partitionBy("tariff", F.year("first_valid_to")).orderBy("first_valid_to")

base_tariffs = (
    df_tariff_windows
    .filter((F.hour(F.col("valid_from")).between(9, 15)) & (F.col("tariff") != "EV"))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
    )
)

qualified = (
    base_tariffs
    .crossJoin(base_tariffs.agg(F.min("rate").alias("overall_min_rate")))
    .filter(F.col("rate") > F.col("overall_min_rate"))
    .drop("overall_min_rate")
)

with_trends = (
    qualified
    .select(
        "id",
        "tariff",
        "valid_from",
        "rate",
        F.lag("valid_to").over(w_tariff_by_rate).alias("prev_valid_to"),
        F.sum("rate").over(w_tariff).alias("tariff_sum_rate"),
        F.dense_rank().over(w_valid_from_to_date_tariff_by).alias("valid_from_dense_rank"),
        F.lag("rate", 3, 0).over(w_valid_from_to_date_tariff_by).alias("prev3_rate"),
        F.first("valid_to").over(w_valid_from_to_date_tariff_by_2).alias("first_valid_to"),
    )
)

annotated = (
    with_trends
    .filter(F.col("prev_valid_to") >= F.lit("2025-01-25 21:00:00").cast("timestamp"))
    .select(
        "*",
        F.date_format(F.col("first_valid_to"), "yyyy-MM-dd").alias("first_valid_to_label"),
        F.upper(F.trim(F.col("tariff"))).alias("tariff_clean"),
        F.round(F.col("tariff_sum_rate") / F.nullif(F.col("rate"), F.lit(0)), 3).alias("tariff_sum_rate_per_rate"),
    )
)

with_trends_2 = (
    annotated
    .select(
        "id",
        "rate",
        "prev_valid_to",
        "valid_from_dense_rank",
        "first_valid_to",
        "first_valid_to_label",
        "tariff_clean",
        "tariff_sum_rate_per_rate",
        F.round(F.col("prev3_rate") / F.nullif(F.sum("prev3_rate").over(w_tariff_valid_from_dense_rank), F.lit(0)), 4).alias("prev3_rate_share_of_tariff_valid_from_dense_rank"),
        F.dense_rank().over(w_first_valid_to_month_by_firs).alias("first_valid_to_dense_rank"),
        F.dense_rank().over(w_tariff_first_valid_to_year_b).alias("first_valid_to_dense_rank_2"),
        F.count(F.lit(1)).over(w_tariff_valid_from_dense_rank).alias("rows_in_tariff_valid_from_dense_rank"),
    )
)

result = (
    with_trends_2
    .select(
        "id",
        "rate",
        "prev_valid_to",
        "valid_from_dense_rank",
        "tariff_clean",
        "prev3_rate_share_of_tariff_valid_from_dense_rank",
        "first_valid_to_dense_rank",
        "first_valid_to_dense_rank_2",
        "rows_in_tariff_valid_from_dense_rank",
        F.abs(F.col("tariff_sum_rate_per_rate") - 5.0).alias("tariff_sum_rate_per_rate_distance"),
        F.length(F.trim(F.col("first_valid_to_label"))).alias("first_valid_to_label_len"),
    )
    .orderBy(F.col("first_valid_to_dense_rank").desc())
)
