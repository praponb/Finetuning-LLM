from pyspark.sql import functions as F

site_rows = (
    df_sites
    .filter(F.col("installed_kw") > 96.9)
    .select(
        F.col("id").alias("qty"),
        F.col("zone").alias("category"),
        F.lit("site").alias("origin"),
    )
)

tariff_side = (
    df_tariff_windows
    .select(
        F.col("id").alias("qty"),
        F.col("tariff").alias("category"),
        F.lit("tariff").alias("origin"),
    )
)

tariff_rows = (
    df_tariff_windows
    .filter(F.col("valid_from") >= F.lit("2025-02-15 04:00:00").cast("timestamp"))
    .select(
        F.col("id").alias("qty"),
        F.col("tariff").alias("category"),
        F.lit("tariff").alias("origin"),
    )
)

result = (
    site_rows
    .union(tariff_side)
    .union(tariff_rows)
)
