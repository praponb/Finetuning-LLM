from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_valid_from = Window.orderBy(F.col("valid_from").desc_nulls_first(), "id")
w_valid_from_year_tariff = Window.partitionBy(F.year("valid_from"), "tariff")
w_tariff_valid_from_month_by_v = Window.partitionBy("tariff", F.month("valid_from")).orderBy(F.col("valid_from").desc(), "id")

prepared = (
    df_tariff_windows
    .filter((F.col("rate").between(2.0, 5.7)) | (F.hour(F.col("valid_to")).between(8, 11)))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
    )
)

with_trends = (
    prepared
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        F.round(F.percent_rank().over(w_all_by_valid_from), 3).alias("valid_from_percent_rank"),
        F.round(F.col("rate") / F.nullif(F.sum("rate").over(w_valid_from_year_tariff), F.lit(0)), 4).alias("rate_share_of_valid_from_year_tariff"),
        F.round((F.unix_timestamp("valid_from") - F.unix_timestamp(F.lag("valid_from").over(w_tariff_valid_from_month_by_v))) / 60, 1).alias("minutes_since_prev_valid_from"),
        F.count(F.lit(1)).over(w_valid_from_year_tariff).alias("rows_in_valid_from_year_tariff"),
        F.round((F.unix_timestamp("valid_to") - F.unix_timestamp(F.lag("valid_to").over(w_tariff_valid_from_month_by_v))) / 60, 1).alias("minutes_since_prev_valid_to"),
    )
)

grouped = (
    with_trends
    .groupBy("tariff")
    .agg(
        F.min("minutes_since_prev_valid_from").alias("min_minutes_since_prev_valid_from"),
        F.countDistinct("tariff").alias("distinct_tariffs"),
        F.max("rate_share_of_valid_from_year_tariff").alias("max_rate_share_of_valid_from_year_tariff"),
    )
)

result = (
    grouped
    .select(
        "tariff",
        "min_minutes_since_prev_valid_from",
        "max_rate_share_of_valid_from_year_tariff",
    )
    .unpivot(
        ids=["tariff"],
        values=["min_minutes_since_prev_valid_from", "max_rate_share_of_valid_from_year_tariff"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
