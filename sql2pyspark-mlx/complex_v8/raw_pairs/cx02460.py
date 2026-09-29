from pyspark.sql import functions as F

site_side = df_sites.select(F.col("id").alias("key_id"), "installed_kw")

tariff_rows = (
    df_tariff_windows
    .filter((F.col("valid_to") < F.lit("2025-03-09 05:00:00").cast("timestamp")) & (F.col("rate") > 2.2))
    .select(F.col("id").alias("key_id"), F.col("rate").alias("installed_kw"))
)

result = (
    site_side
    .union(tariff_rows)
    .orderBy(F.col("installed_kw").desc(), "key_id")
)
