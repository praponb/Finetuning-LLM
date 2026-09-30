from pyspark.sql import functions as F

recent = (
    df_tariff_windows
    .filter(F.month(F.col("valid_from")).isin(1, 2))
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
    )
)

filtered = (
    recent
    .crossJoin(recent.agg(F.avg("rate").alias("overall_avg_rate")))
    .filter(F.col("rate") > F.col("overall_avg_rate"))
    .drop("overall_avg_rate")
)

summary = (
    filtered
    .groupBy(
        F.year("valid_from").alias("valid_from_year"),
        F.year("valid_to").alias("valid_to_year"),
    )
    .agg(
        F.min("valid_from").alias("first_valid_from"),
        F.min("valid_to").alias("first_valid_to"),
    )
)

result = (
    summary
    .filter(F.hour(F.col("first_valid_to")).between(7, 10))
    .select(
        "valid_from_year",
        "valid_to_year",
        "first_valid_to",
        F.least(F.col("valid_from_year"), F.col("valid_to_year")).alias("least_valid_from_year_valid_to_year"),
        F.date_trunc("year", F.col("first_valid_to")).alias("first_valid_to_year"),
    )
    .orderBy(
        F.col("least_valid_from_year_valid_to_year").asc_nulls_last(),
        F.col("valid_to_year").desc(),
    )
)
