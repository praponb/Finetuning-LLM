from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_valid_from_year_ties = Window.orderBy("valid_from_year")
w_valid_from_year = Window.partitionBy("valid_from_year")

valid = (
    df_tariff_windows
    .filter(F.col("tariff") == "EV")
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.concat(F.col("tariff"), F.lit("#"), F.col("id").cast("string")).alias("tariff_ref"),
        F.year(F.col("valid_from")).alias("valid_from_year"),
    )
)

ranked = (
    valid
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_valid_from_year_ties), 3).alias("valid_from_year_percent_rank"),
        F.max("valid_from_year").over(w_valid_from_year).alias("valid_from_year_max_valid_from_year"),
    )
)

screened = (
    ranked
    .crossJoin(ranked.agg(F.avg("valid_from_year").alias("overall_avg_valid_from_year")))
    .filter(F.col("valid_from_year") > F.col("overall_avg_valid_from_year"))
    .drop("overall_avg_valid_from_year")
)

enriched = (
    screened
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "tariff_ref",
        "valid_from_year_percent_rank",
        "valid_from_year_max_valid_from_year",
        F.abs(F.col("rate") - 5.2).alias("rate_distance"),
    )
)

result = (
    enriched
    .select(
        "id",
        "tariff",
        "valid_to",
        "tariff_ref",
        "valid_from_year_percent_rank",
    )
)
