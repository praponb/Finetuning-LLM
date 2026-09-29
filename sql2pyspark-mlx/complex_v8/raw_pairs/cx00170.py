from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_valid_from_month = Window.partitionBy(F.month("valid_from"))
w_valid_to_month = Window.partitionBy(F.month("valid_to"))
w_valid_to_to_date_tariff_by_v = Window.partitionBy(F.to_date("valid_to"), "tariff").orderBy(F.col("valid_from").desc_nulls_first(), "id")
w_valid_from_month_by_rate = Window.partitionBy(F.month("valid_from")).orderBy("rate", "id")
w_tariff_by_valid_from = Window.partitionBy("tariff").orderBy("valid_from", "id")
w_all_by_valid_from_full = Window.orderBy(F.col("valid_from").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_valid_from_year_tariff = Window.partitionBy(F.year("valid_from"), "tariff")
w_all_by_valid_from = Window.orderBy("valid_from", "id")
w_all_by_valid_from_month_sum = Window.orderBy("valid_from_month_sum_rate", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_next2_valid_to_month_valid_f = Window.partitionBy("next2_valid_to_month", "valid_from_tercile")
w_all_by_valid_from_month_sum_2 = Window.orderBy("valid_from_month_sum_rate")

recent_tariffs = (
    df_tariff_windows
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.substring(F.trim(F.col("tariff")), 1, 4).alias("tariff_prefix"),
        F.upper(F.trim(F.col("tariff"))).alias("tariff_clean"),
    )
)

ranked = (
    recent_tariffs
    .select(
        "*",
        F.round(F.col("rate") - F.avg("rate").over(w_valid_from_month), 2).alias("rate_vs_valid_from_month_avg"),
        F.count(F.lit(1)).over(w_valid_to_month).alias("rows_in_valid_to_month"),
        F.ntile(4).over(w_valid_to_to_date_tariff_by_v).alias("valid_from_quartile"),
        F.sum("rate").over(w_valid_from_month).alias("valid_from_month_sum_rate"),
        F.round((F.unix_timestamp("valid_from") - F.unix_timestamp(F.lag("valid_from").over(w_valid_from_month_by_rate))) / 60, 1).alias("minutes_since_prev_valid_from"),
        F.lead("valid_to", 2).over(w_tariff_by_valid_from).alias("next2_valid_to"),
        F.round(F.col("rate") / F.nullif(F.sum("rate").over(w_valid_from_month), F.lit(0)), 4).alias("rate_share_of_valid_from_month"),
        F.first("valid_to").over(w_all_by_valid_from_full).alias("first_valid_to"),
        F.round(F.avg("rate").over(w_valid_from_year_tariff), 2).alias("valid_from_year_tariff_avg_rate"),
        F.ntile(3).over(w_all_by_valid_from).alias("valid_from_tercile"),
        F.nth_value("valid_to", 2).over(w_all_by_valid_from_full).alias("second_valid_to"),
        F.round((F.unix_timestamp("valid_to") - F.unix_timestamp(F.lag("valid_to").over(w_valid_from_month_by_rate))) / 60, 1).alias("minutes_since_prev_valid_to"),
        F.round(F.avg("rate").over(w_valid_from_month), 2).alias("valid_from_month_avg_rate"),
        F.sum("rate").over(w_valid_to_month).alias("valid_to_month_sum_rate"),
    )
)

tagged = ranked.select("*", F.month(F.col("next2_valid_to")).alias("next2_valid_to_month"))

sequenced = (
    tagged
    .select(
        "id",
        "tariff",
        "valid_from",
        "rate",
        "tariff_prefix",
        "rate_vs_valid_from_month_avg",
        "rows_in_valid_to_month",
        "valid_from_quartile",
        "valid_from_month_sum_rate",
        "minutes_since_prev_valid_from",
        "rate_share_of_valid_from_month",
        "valid_from_tercile",
        "second_valid_to",
        "valid_from_month_avg_rate",
        "valid_to_month_sum_rate",
        F.first("second_valid_to").over(w_all_by_valid_from_month_sum).alias("first_second_valid_to"),
        F.min("rows_in_valid_to_month").over(w_next2_valid_to_month_valid_f).alias("next2_valid_to_month_valid_from_tercile_min_rows"),
        F.round(F.cume_dist().over(w_all_by_valid_from_month_sum_2), 3).alias("valid_from_month_sum_rate_cume_dist"),
    )
)

stats = (
    sequenced
    .groupBy(
        F.year("valid_from").alias("valid_from_year"),
        F.year("first_second_valid_to").alias("first_second_valid_to_year"),
    )
    .agg(
        F.min("valid_from").alias("first_valid_from"),
        F.round(F.sum("valid_from_quartile") / F.nullif(F.sum("valid_to_month_sum_rate"), F.lit(0)), 3).alias("valid_from_quartile_to_valid_to_month_sum_rate_r"),
        F.round(F.stddev("rate_vs_valid_from_month_avg"), 2).alias("stddev_rate_vs_valid_from_month_avg"),
        F.round(F.stddev("next2_valid_to_month_valid_from_tercile_min_rows"), 2).alias("stddev_next2_valid_to_month_valid_from_tercile_m"),
    )
)

result = (
    stats
    .select(
        "valid_from_year",
        "first_second_valid_to_year",
        "valid_from_quartile_to_valid_to_month_sum_rate_r",
        "stddev_rate_vs_valid_from_month_avg",
        "stddev_next2_valid_to_month_valid_from_tercile_m",
        F.year(F.col("first_valid_from")).alias("first_valid_from_year"),
        F.year(F.col("first_valid_from")).alias("first_valid_from_year_2"),
    )
    .orderBy(
        F.col("first_valid_from_year_2").asc_nulls_last(),
        "valid_from_year",
        "first_second_valid_to_year",
    )
    .limit(20)
)
