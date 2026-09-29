from pyspark.sql import functions as F

base_tariffs = (
    df_tariff_windows
    .filter(F.col("tariff").isNotNull())
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
        F.lower(F.trim(F.col("tariff"))).alias("tariff_clean"),
        F.coalesce(F.col("rate"), F.lit(0.0)).alias("rate_filled"),
    )
)

screened = (
    base_tariffs
    .crossJoin(base_tariffs.agg(F.avg("rate").alias("overall_avg_rate")))
    .filter(F.col("rate") > F.col("overall_avg_rate"))
    .drop("overall_avg_rate")
)

screened_2 = (
    screened
    .crossJoin(screened.agg(F.max("rate").alias("overall_max_rate")))
    .filter(F.col("rate") < F.col("overall_max_rate"))
    .drop("overall_max_rate")
)

tagged = (
    screened_2
    .filter(F.col("rate") <= 5.7)
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
        "tariff_clean",
        F.round(F.col("rate_filled") * 0.25, 2).alias("rate_filled_adj"),
        F.coalesce(F.col("rate_filled"), F.lit(0.0)).alias("rate_filled_filled"),
        F.date_format(F.col("valid_from"), "yyyy-MM-dd").alias("valid_from_label"),
    )
)

result = (
    tagged
    .select(
        "id",
        "rate_filled_adj",
        "rate_filled_filled",
        "rate",
    )
    .unpivot(
        ids=["id"],
        values=["rate_filled_adj", "rate_filled_filled", "rate"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
