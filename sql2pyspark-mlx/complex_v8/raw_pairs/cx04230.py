from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_valid_from_month_valid_to_ye = Window.partitionBy(F.month("valid_from"), F.to_date("valid_to_year"))
w_valid_to_to_date_tariff_by_h = Window.partitionBy(F.to_date("valid_to"), "tariff").orderBy(F.col("hours_valid_from_to_valid_to").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_valid_to = Window.orderBy("valid_to", "id")
w_all_by_valid_from = Window.orderBy("valid_from", "id")
w_valid_to_to_date_tariff_by_h_2 = Window.partitionBy(F.to_date("valid_to"), "tariff").orderBy(F.col("hours_valid_from_to_valid_to").asc_nulls_last(), "id")
w_valid_to_to_date = Window.partitionBy(F.to_date("valid_to"))
w_tariff_by_hours_valid_from_t = Window.partitionBy("tariff").orderBy("hours_valid_from_to_valid_to", "id")
w_valid_to_to_date_tariff_by_h_3 = Window.partitionBy(F.to_date("valid_to"), "tariff").orderBy(F.col("hours_valid_from_to_valid_to").asc_nulls_last())
w_tariff_by_hours_valid_from_t_2 = Window.partitionBy("tariff").orderBy("hours_valid_from_to_valid_to")

scoped_tariffs = (
    df_tariff_windows
    .filter(F.col("valid_to") >= F.lit("2025-02-18 04:00:00").cast("timestamp"))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        F.round((F.unix_timestamp(F.col("valid_to")) - F.unix_timestamp(F.col("valid_from"))) / 3600, 1).alias("hours_valid_from_to_valid_to"),
        F.date_trunc("year", F.col("valid_to")).alias("valid_to_year"),
    )
)

windowed = (
    scoped_tariffs
    .select(
        "*",
        F.count(F.lit(1)).over(w_valid_from_month_valid_to_ye).alias("rows_in_valid_from_month_valid_to_year_to_date"),
        F.min("hours_valid_from_to_valid_to").over(w_valid_to_to_date_tariff_by_h).alias("min_hours_valid_from_to_valid_to_so_far"),
        F.row_number().over(w_all_by_valid_to).alias("rn"),
        F.round(F.col("hours_valid_from_to_valid_to") / F.nullif(F.sum("hours_valid_from_to_valid_to").over(w_valid_from_month_valid_to_ye), F.lit(0)), 4).alias("hours_valid_from_to_valid_to_share_of_valid_from"),
        F.lead("valid_to").over(w_all_by_valid_from).alias("next_valid_to"),
        F.ntile(10).over(w_valid_to_to_date_tariff_by_h_2).alias("hours_valid_from_to_valid_to_decile"),
        F.round(F.col("hours_valid_from_to_valid_to") / F.nullif(F.sum("hours_valid_from_to_valid_to").over(w_valid_to_to_date), F.lit(0)), 4).alias("hours_valid_from_to_valid_to_share_of_valid_to_t"),
        F.lead("valid_from").over(w_tariff_by_hours_valid_from_t).alias("next_valid_from"),
        F.rank().over(w_valid_to_to_date_tariff_by_h_3).alias("hours_valid_from_to_valid_to_rank"),
        F.first("valid_to_year", ignorenulls=True).over(w_valid_to_to_date_tariff_by_h).alias("first_known_valid_to_year"),
        F.lag("valid_to_year").over(w_all_by_valid_from).alias("prev_valid_to_year"),
        F.max("hours_valid_from_to_valid_to").over(w_valid_from_month_valid_to_ye).alias("valid_from_month_valid_to_year_to_date_max_hours"),
        F.dense_rank().over(w_tariff_by_hours_valid_from_t_2).alias("hours_valid_from_to_valid_to_dense_rank"),
        F.round(F.avg("hours_valid_from_to_valid_to").over(w_valid_to_to_date), 2).alias("valid_to_to_date_avg_hours_valid_from_to_valid_t"),
    )
)

grouped = (
    windowed
    .groupBy(F.date_trunc("month", "valid_from").alias("valid_from_month"))
    .agg(
        F.max("valid_from").alias("last_valid_from"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("tariff").isNotNull(), 1).otherwise(0)).alias("n_tariff_is_not_null"),
        F.sum(F.when(F.col("tariff") != "FLAT", F.col("hours_valid_from_to_valid_to_share_of_valid_to_t")).otherwise(0)).alias("hours_valid_from_to_valid_to_share_of_valid_to_t_2"),
        F.sum(F.when(F.col("tariff") == "FLAT", 1).otherwise(0)).alias("n_tariff_flat"),
        F.count("*").alias("cnt"),
    )
)

result = (
    grouped
    .select(
        "valid_from_month",
        "last_valid_from",
        "distinct_ids",
        "n_tariff_is_not_null",
        "hours_valid_from_to_valid_to_share_of_valid_to_t_2",
        "cnt",
    )
    .orderBy(F.col("distinct_ids").asc_nulls_last())
)
