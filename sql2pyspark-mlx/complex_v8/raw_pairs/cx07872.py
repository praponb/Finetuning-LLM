from pyspark.sql import functions as F

site_side = (
    df_sites
    .select(
        F.lit("site").alias("origin"),
        F.col("id").alias("qty"),
        F.col("installed_kw").alias("amount"),
    )
)

tariff_rows = (
    df_tariff_windows
    .filter((F.col("tariff") == "TOU") & (F.col("valid_to") >= F.lit("2025-02-16 05:00:00").cast("timestamp")))
    .select(F.lit("tariff").alias("origin"), "id", "rate")
)

result = (
    site_side
    .union(tariff_rows)
    .orderBy("qty", "amount")
)
