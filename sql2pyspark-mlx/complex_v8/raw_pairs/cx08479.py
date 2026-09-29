from pyspark.sql import functions as F

site_rows = (
    df_sites
    .filter(F.col("zone").isin("Z4", "Z2"))
    .select(F.col("tariff").alias("category"))
)

reading_side = (
    df_meter_readings
    .filter(F.col("quality").isin("BAD", "EST"))
    .select(F.col("quality").alias("category"))
)

result = site_rows.subtract(reading_side)
