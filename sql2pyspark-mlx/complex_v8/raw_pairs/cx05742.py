from pyspark.sql import functions as F

reading_side = (
    df_meter_readings
    .filter(~F.col("quality").isin("EST", "OK"))
    .select("quality")
)

site_side = df_sites.select("zone")

result = (
    reading_side
    .exceptAll(site_side)
    .orderBy(F.col("quality").desc())
)
