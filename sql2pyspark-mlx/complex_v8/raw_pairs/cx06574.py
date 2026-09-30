from pyspark.sql import functions as F

recent = (
    df_tariff_windows
    .filter(F.col("valid_to") >= F.lit("2025-02-15 04:00:00").cast("timestamp"))
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
        F.round((F.unix_timestamp(F.col("valid_to")) - F.unix_timestamp(F.col("valid_from"))) / 3600, 1).alias("hours_valid_from_to_valid_to"),
        F.lower(F.regexp_replace(F.trim(F.col("tariff")), " ", "_")).alias("tariff_slug"),
    )
)

enriched = (
    recent
    .filter(F.col("rate") > 3.7)
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("tariff_slug")), " ", "_")).alias("tariff_slug_slug"),
        F.round((F.unix_timestamp(F.col("valid_from")) - F.unix_timestamp(F.col("valid_to"))) / 3600, 1).alias("hours_valid_to_to_valid_from"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "valid_to",
        "rate",
        "tariff_slug",
        "hours_valid_to_to_valid_from",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("valid_to")).alias("days_since_valid_to"),
    )
)

labeled = (
    scored
    .select(
        "id",
        "valid_to",
        "rate",
        "tariff_slug",
        "days_since_valid_to",
        F.date_trunc("hour", F.col("valid_to")).alias("valid_to_hour"),
        F.date_format(F.col("valid_to"), "yyyy-MM-dd HH:00").alias("valid_to_label"),
    )
)

result = (
    labeled
    .select(
        "id",
        "days_since_valid_to",
        "valid_to_hour",
        "valid_to_label",
    )
    .orderBy(F.col("valid_to_label").asc_nulls_last(), F.col("valid_to_hour").desc())
)
