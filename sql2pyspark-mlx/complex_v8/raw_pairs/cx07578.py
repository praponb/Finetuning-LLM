from pyspark.sql import functions as F

reading_rows = (
    df_meter_readings
    .filter((F.col("quality").isNotNull()) & (F.col("site_id").isNotNull()))
    .select(F.col("quality").alias("name"))
)

tariff_rows = df_tariff_windows.select("tariff")

result = (
    reading_rows
    .intersect(tariff_rows)
    .orderBy(F.col("name").desc())
)
