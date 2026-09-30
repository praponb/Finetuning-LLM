from pyspark.sql import functions as F

base_tariffs = (
    df_tariff_windows
    .filter((F.col("rate") >= 3.0) | (F.col("tariff") == "EV"))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.round((F.unix_timestamp(F.col("valid_to")) - F.unix_timestamp(F.col("valid_from"))) / 60, 1).alias("minutes_valid_from_to_valid_to"),
        F.to_date(F.col("valid_from")).alias("valid_from_date"),
    )
)

filtered = (
    base_tariffs
    .crossJoin(base_tariffs.agg(F.max("minutes_valid_from_to_valid_to").alias("overall_max_minutes_valid_from_to_valid_to")))
    .filter(F.col("minutes_valid_from_to_valid_to") < F.col("overall_max_minutes_valid_from_to_valid_to"))
    .drop("overall_max_minutes_valid_from_to_valid_to")
)

derived = (
    filtered
    .select(
        "*",
        F.last_day(F.col("valid_from_date")).alias("valid_from_date_month_end"),
        F.lower(F.regexp_replace(F.trim(F.col("tariff")), " ", "_")).alias("tariff_slug"),
    )
)

result = (
    derived
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        "minutes_valid_from_to_valid_to",
        "valid_from_date",
        "valid_from_date_month_end",
        "tariff_slug",
        F.abs(F.col("rate") - 5.5).alias("rate_distance"),
    )
    .orderBy(
        F.col("minutes_valid_from_to_valid_to").asc_nulls_last(),
        F.col("tariff").desc(),
    )
)
