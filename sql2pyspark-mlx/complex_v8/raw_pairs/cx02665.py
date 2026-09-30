from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_valid_to_year_by_valid_to_mo = Window.partitionBy(F.year("valid_to")).orderBy("valid_to", "id").rowsBetween(-3, Window.currentRow)
w_tariff = Window.partitionBy("tariff")
w_valid_to_to_date_tariff_by_r = Window.partitionBy(F.to_date("valid_to"), "tariff").orderBy("rate", "id")
w_latest = Window.partitionBy("tariff").orderBy(F.col("rows_in_tariff").desc(), F.col("id").desc())

scoped = (
    df_tariff_windows
    .filter((F.col("valid_from") < F.lit("2025-03-09 04:00:00").cast("timestamp")) & (F.col("rate") > 5.2))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.concat(F.col("tariff"), F.lit("#"), F.col("id").cast("string")).alias("tariff_ref"),
        F.date_format(F.col("valid_to"), "yyyy-MM-dd HH:00").alias("valid_to_label"),
    )
)

with_trends = (
    scoped
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        "valid_to_label",
        F.round(F.avg("rate").over(w_valid_to_year_by_valid_to_mo), 2).alias("rate_avg_4"),
        F.count(F.lit(1)).over(w_tariff).alias("rows_in_tariff"),
        F.ntile(3).over(w_valid_to_to_date_tariff_by_r).alias("rate_tercile"),
        F.round(F.col("rate") / F.nullif(F.sum("rate").over(w_tariff), F.lit(0)), 4).alias("rate_share_of_tariff"),
    )
)

latest = (
    with_trends
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest
    .select(
        "id",
        "tariff",
        "valid_from",
        "rate",
        "rows_in_tariff",
        "rate_tercile",
        "rate_share_of_tariff",
        F.least(F.col("rows_in_tariff"), F.col("rate_share_of_tariff")).alias("least_rows_in_tariff_rate_share_of_tariff"),
    )
    .orderBy("rows_in_tariff")
)
