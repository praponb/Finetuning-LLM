from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_by_valid_from_ties = Window.partitionBy("tariff").orderBy(F.col("valid_from").asc_nulls_last())
w_valid_from_to_date_tariff = Window.partitionBy(F.to_date("valid_from"), "tariff")
w_tariff_by_valid_from_running = Window.partitionBy("tariff").orderBy(F.col("valid_from").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_tariffs = (
    df_tariff_windows
    .filter(F.col("tariff").isin("TOU", "FLAT"))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.lower(F.regexp_replace(F.trim(F.col("tariff")), " ", "_")).alias("tariff_slug"),
        F.concat(F.col("tariff"), F.lit("#"), F.col("id").cast("string")).alias("tariff_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("tariff")), " ", "_")).alias("tariff_slug_2"),
    )
)

screened = (
    valid_tariffs
    .crossJoin(valid_tariffs.agg(F.avg("rate").alias("overall_avg_rate")))
    .filter(F.col("rate") > F.col("overall_avg_rate"))
    .drop("overall_avg_rate")
)

without_matches = (
    screened
    .crossJoin(screened.agg(F.avg("rate").alias("overall_avg_rate")))
    .filter(F.col("rate") > F.col("overall_avg_rate"))
    .drop("overall_avg_rate")
)

windowed = (
    without_matches
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "tariff_slug",
        "tariff_ref",
        F.dense_rank().over(w_tariff_by_valid_from_ties).alias("valid_from_dense_rank"),
        F.sum("rate").over(w_valid_from_to_date_tariff).alias("valid_from_to_date_tariff_sum_rate"),
        F.last("valid_to", ignorenulls=True).over(w_tariff_by_valid_from_running).alias("last_known_valid_to"),
    )
)

result = (
    windowed
    .groupingSets(
        [["valid_from_dense_rank", "tariff"], ["valid_from_dense_rank"], []],
        "valid_from_dense_rank", "tariff",
    )
    .agg(
        F.sum("valid_from_dense_rank").alias("total_valid_from_dense_rank"),
        F.sum(F.when(~F.col("tariff").isin("FLAT", "TOU"), F.col("valid_from_dense_rank")).otherwise(0)).alias("valid_from_dense_rank_tariff_not_in_flat_tou"),
    )
    .select(
        "valid_from_dense_rank",
        F.coalesce(F.col("tariff"), F.lit("ALL")).alias("tariff"),
        "total_valid_from_dense_rank",
        "valid_from_dense_rank_tariff_not_in_flat_tou",
    )
)
