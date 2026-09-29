from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_valid_to_year_filled_by_vali = Window.partitionBy("valid_to_year_filled").orderBy(F.col("valid_to_year_filled").desc(), "valid_from_day", "valid_to_year")
w_valid_to_year_by_valid_to_ye = Window.partitionBy("valid_to_year").orderBy(F.col("valid_to_year_filled").desc(), "valid_from_day", "valid_to_year")
w_valid_to_year_filled = Window.partitionBy("valid_to_year_filled")
w_all_by_valid_to_year_filled = Window.orderBy("valid_to_year_filled", "valid_from_day", "valid_to_year").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_valid_from_day_year_month = Window.partitionBy(F.date_trunc("month", "valid_from_day_year"))
w_all_by_stddev_rate = Window.orderBy(F.col("stddev_rate").desc(), "valid_from_day", "valid_to_year")

scoped = (
    df_tariff_windows
    .filter(F.col("tariff").isin("FLAT", "TOU"))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown"),
        F.length(F.trim(F.col("tariff"))).alias("tariff_len"),
    )
)

rollup_base = (
    scoped
    .groupBy(
        F.to_date("valid_from").alias("valid_from_day"),
        F.year("valid_to").alias("valid_to_year"),
    )
    .agg(
        F.min("valid_from").alias("first_valid_from"),
        F.round(F.stddev("rate"), 2).alias("stddev_rate"),
        F.min("rate").alias("min_rate"),
    )
)

derived = (
    rollup_base
    .select(
        "*",
        F.round(F.col("min_rate") / F.nullif(F.col("valid_to_year"), F.lit(0)), 3).alias("min_rate_per_valid_to_year"),
        F.coalesce(F.col("valid_to_year"), F.lit(0)).alias("valid_to_year_filled"),
    )
)

sequenced = (
    derived
    .select(
        "*",
        F.round(F.cume_dist().over(w_valid_to_year_filled_by_vali), 3).alias("valid_to_year_filled_cume_dist"),
        F.row_number().over(w_valid_to_year_by_valid_to_ye).alias("rn"),
        F.round(F.col("min_rate") / F.nullif(F.sum("min_rate").over(w_valid_to_year_filled), F.lit(0)), 4).alias("min_rate_share_of_valid_to_year_filled"),
        F.lag("min_rate_per_valid_to_year", 2).over(w_valid_to_year_filled_by_vali).alias("prev2_min_rate_per_valid_to_year"),
        F.first("valid_from_day", ignorenulls=True).over(w_all_by_valid_to_year_filled).alias("first_known_valid_from_day"),
    )
)

scored = (
    sequenced
    .select(
        "*",
        F.hour(F.col("first_valid_from")).alias("first_valid_from_hour"),
        F.date_trunc("year", F.col("valid_from_day")).alias("valid_from_day_year"),
    )
)

ranked = (
    scored
    .withColumn("rows_in_valid_from_day_year_month", F.count(F.lit(1)).over(w_valid_from_day_year_month))
    .withColumn("stddev_rate_rank", F.rank().over(w_all_by_stddev_rate))
)

result = (
    ranked
    .select(
        "valid_from_day",
        "first_valid_from",
        "stddev_rate",
        "min_rate_per_valid_to_year",
        "valid_to_year_filled",
        "valid_to_year_filled_cume_dist",
        "rn",
        "min_rate_share_of_valid_to_year_filled",
        "first_known_valid_from_day",
        "first_valid_from_hour",
        "valid_from_day_year",
        "rows_in_valid_from_day_year_month",
        "stddev_rate_rank",
        (F.col("first_valid_from") + F.expr("INTERVAL 30 MINUTES")).alias("first_valid_from_plus_30m"),
        F.abs(F.col("stddev_rate_rank") - 4).alias("stddev_rate_rank_distance"),
    )
    .orderBy(
        F.col("rn").asc_nulls_last(),
        F.col("first_valid_from").desc_nulls_first(),
    )
)
