from pyspark.sql import functions as F

reading_rows = (
    df_meter_readings
    .filter(F.col("quality").isin("EST", "BAD"))
    .select("quality")
)

site_side = df_sites.select("tariff")

result = (
    reading_rows
    .intersect(site_side)
    .orderBy(F.col("quality").desc())
)
