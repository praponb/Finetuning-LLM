from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff = Window.partitionBy("tariff")
w_valid_to_to_date_by_valid_to = Window.partitionBy(F.to_date("valid_to")).orderBy("valid_to", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_valid_to_to_date_by_valid_to_2 = Window.partitionBy(F.to_date("valid_to")).orderBy("valid_to", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_tariffs = (
    df_tariff_windows
    .select(
        "id",
        "tariff",
        "valid_to",
        "rate",
    )
)

with_trends = (
    prepared_tariffs
    .select(
        "id",
        "tariff",
        "valid_to",
        "rate",
        F.max("rate").over(w_tariff).alias("tariff_max_rate"),
        F.sum("rate").over(w_valid_to_to_date_by_valid_to).alias("running_rate"),
        F.nth_value("valid_to", 2).over(w_valid_to_to_date_by_valid_to_2).alias("second_valid_to"),
        F.sum("rate").over(w_tariff).alias("tariff_sum_rate"),
    )
)

grouped = (
    with_trends
    .groupBy(
        "tariff",
        F.when(F.col("tariff_max_rate") >= 52.7, "A").when(F.col("tariff_max_rate") >= 29.3, "B").otherwise("C").alias("tariff_max_rate_tier"),
        F.to_date("second_valid_to").alias("second_valid_to_day"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("tariff") != "FLAT", F.col("tariff_sum_rate")).otherwise(0)).alias("tariff_sum_rate_tariff_flat"),
        F.round(F.sum("running_rate"), 2).alias("total_running_rate"),
        F.round(F.sum("tariff_sum_rate") / F.nullif(F.sum("running_rate"), F.lit(0)), 3).alias("tariff_sum_rate_to_running_rate_ratio"),
    )
)

result = (
    grouped
    .groupBy("cnt", "distinct_ids")
    .pivot("tariff", ["TOU", "FLAT"])
    .agg(F.sum("tariff_sum_rate_tariff_flat"))
)
