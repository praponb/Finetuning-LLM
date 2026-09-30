from pyspark.sql import functions as F

reading_rows = df_meter_readings.select("id", "quality", "kwh")

site_rows = (
    df_sites
    .filter((F.col("site_name").isNotNull()) & (F.col("installed_kw").between(96.9, 446.9)))
    .select("id", "zone", "installed_kw")
)

reading_rows_2 = (
    df_meter_readings
    .filter(F.col("kwh") < 101.2)
    .select("id", "quality", "kwh")
)

result = (
    reading_rows
    .union(site_rows)
    .union(reading_rows_2)
    .orderBy("quality", "kwh")
)
