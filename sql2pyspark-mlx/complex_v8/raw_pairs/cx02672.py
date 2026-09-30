from pyspark.sql import functions as F

site_rows = (
    df_sites
    .filter((F.col("commissioned_on").between(F.lit("2024-06-27").cast("date"), F.lit("2025-05-07").cast("date"))) & (F.col("tariff").isNotNull()))
    .select("id", "site_name")
)

reading_rows = (
    df_meter_readings
    .select(F.col("site_id").alias("id"), F.col("quality").alias("site_name"))
)

result = (
    site_rows
    .intersect(reading_rows)
    .orderBy("site_name", "id")
)
