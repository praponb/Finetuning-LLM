from pyspark.sql import functions as F

site_side = (
    df_sites
    .filter((F.length(F.trim(F.col("site_name"))) >= 8) & (F.col("zone") == "Z4"))
    .select("id")
)

reading_rows = df_meter_readings.select("id")

result = (
    site_side
    .exceptAll(reading_rows)
    .orderBy(F.col("id").desc())
)
