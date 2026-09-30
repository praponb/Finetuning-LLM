from pyspark.sql import functions as F

reading_side = df_meter_readings.select("site_id", F.col("kwh").alias("amount"))

site_side = (
    df_sites
    .filter((F.col("installed_kw") >= 141.6) & (F.col("tariff").isin("FLAT", "TOU")))
    .select(F.col("id").alias("site_id"), F.col("installed_kw").alias("amount"))
)

result = reading_side.unionByName(site_side)
