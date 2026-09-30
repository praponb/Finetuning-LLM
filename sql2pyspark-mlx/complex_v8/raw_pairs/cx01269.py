from pyspark.sql import functions as F

tariff_side = (
    df_tariff_windows
    .filter((F.col("rate") < 5.9) & (F.hour(F.col("valid_from")).between(8, 12)))
    .select("id", F.col("tariff").alias("name"))
)

site_side = df_sites.select("id", "site_name")

result = tariff_side.subtract(site_side)
