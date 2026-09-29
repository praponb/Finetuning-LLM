from pyspark.sql import functions as F

prepared_tariffs = (
    df_tariff_windows
    .filter(F.col("rate") >= 3.0)
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
    )
)

annotated = (
    prepared_tariffs
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.length(F.trim(F.col("tariff"))).alias("tariff_len"),
        F.concat(F.col("tariff"), F.lit("#"), F.col("id").cast("string")).alias("tariff_ref"),
        F.concat(F.col("tariff"), F.lit("#"), F.col("id").cast("string")).alias("tariff_ref_2"),
    )
)

annotated_2 = (
    annotated
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        "tariff_len",
        "tariff_ref",
        "tariff_ref_2",
        F.to_date(F.col("valid_from")).alias("valid_from_date"),
    )
)

annotated_3 = (
    annotated_2
    .select(
        "*",
        F.date_format(F.col("valid_from"), "MMM yyyy").alias("valid_from_label"),
    )
)

result = (
    annotated_3
    .groupingSets(
        [["tariff", "tariff_len"], ["tariff"], ["tariff_len"], []],
        "tariff", "tariff_len",
    )
    .agg(
        F.grouping("tariff").alias("is_total_tariff"),
        F.count("*").alias("cnt"),
    )
    .select(
        F.coalesce(F.col("tariff"), F.lit("ALL")).alias("tariff"),
        "tariff_len",
        "is_total_tariff",
        "cnt",
    )
)
