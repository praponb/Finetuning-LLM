from pyspark.sql import functions as F

valid_tariffs = (
    df_tariff_windows
    .filter(F.col("tariff").isNotNull())
    .select(
        "id",
        "tariff",
        "valid_from",
        "rate",
        F.date_format(F.col("valid_from"), "yyyy-MM").alias("valid_from_label"),
    )
)

tariff_side = (
    valid_tariffs
    .filter((F.col("valid_from_label").isNotNull()) & (F.col("valid_from") >= F.lit("2025-01-24 14:00:00").cast("timestamp")))
    .select(
        F.lit("tariff").alias("src"),
        F.col("id").alias("key_id"),
        "valid_from_label",
    )
)

site_rows = (
    df_sites
    .filter(F.col("installed_kw").between(66.5, 386.6))
    .select(
        F.lit("site").alias("src"),
        F.col("id").alias("key_id"),
        F.col("zone").alias("valid_from_label"),
    )
)

result = (
    tariff_side
    .unionByName(site_rows)
    .distinct()
    .orderBy(F.col("src").desc())
)
