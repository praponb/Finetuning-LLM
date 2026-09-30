from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("tariff").orderBy(F.col("rate_filled").desc(), F.col("id").desc())
w_tariff = Window.partitionBy("tariff")
w_all_by_rate_full = Window.orderBy(F.col("rate").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_tariff_windows
    .filter(F.hour(F.col("valid_to")).between(7, 13))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.coalesce(F.col("rate"), F.lit(0.0)).alias("rate_filled"),
        F.date_format(F.col("valid_to"), "yyyy-MM-dd HH:00").alias("valid_to_label"),
        F.abs(F.col("rate") - 4.2).alias("rate_distance"),
    )
)

latest_per_key = (
    valid
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

metrics = (
    latest_per_key
    .select(
        "*",
        F.min("rate_distance").over(w_tariff).alias("tariff_min_rate_distance"),
        F.nth_value("rate_distance", 2).over(w_all_by_rate_full).alias("second_rate_distance"),
        F.count(F.lit(1)).over(w_tariff).alias("rows_in_tariff"),
        F.last("rate_distance").over(w_all_by_rate_full).alias("last_rate_distance"),
    )
)

result = (
    metrics
    .select(
        "id",
        "valid_to",
        "rate_filled",
        "valid_to_label",
        "rate_distance",
        "tariff_min_rate_distance",
        "second_rate_distance",
        "rows_in_tariff",
        "last_rate_distance",
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown"),
    )
    .orderBy(
        F.col("tariff_min_rate_distance").desc_nulls_first(),
        F.col("rate_filled").desc(),
        "id",
    )
    .limit(20)
)
