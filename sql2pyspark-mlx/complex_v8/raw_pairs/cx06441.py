from pyspark.sql import functions as F

filtered = (
    df_tariff_windows
    .select(
        "id",
        "tariff",
        "valid_from",
        "rate",
        F.coalesce(F.col("rate"), F.lit(0.0)).alias("rate_filled"),
        F.date_format(F.col("valid_from"), "yyyy-MM-dd HH:00").alias("valid_from_label"),
        F.lower(F.regexp_replace(F.trim(F.col("tariff")), " ", "_")).alias("tariff_slug"),
    )
)

grouped = (
    filtered
    .filter(F.col("valid_from_label").isNotNull())
    .groupBy(F.to_date("valid_from").alias("valid_from_day"), "tariff")
    .agg(
        F.countDistinct("tariff").alias("distinct_tariffs"),
        F.sum(F.when(~F.col("tariff").isin("FLAT", "TOU"), 1).otherwise(0)).alias("n_tariff_not_in_flat_tou"),
        F.sum(F.when(F.col("tariff") == "EV", F.col("rate")).otherwise(0)).alias("rate_tariff_ev"),
        F.round(F.stddev("rate"), 2).alias("stddev_rate"),
        F.round(F.stddev("rate_filled"), 2).alias("stddev_rate_filled"),
        F.round(F.avg("rate"), 2).alias("avg_rate"),
    )
)

result = (
    grouped
    .groupBy("valid_from_day")
    .pivot("tariff", ["EV", "TOU"])
    .agg(F.count("distinct_tariffs"))
)
