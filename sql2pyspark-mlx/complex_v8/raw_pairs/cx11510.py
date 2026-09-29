from pyspark.sql import functions as F

site_side = (
    df_sites
    .filter(F.col("zone") == "Z4")
    .select("zone")
)

reading_side = (
    df_meter_readings
    .filter((F.to_date(F.col("read_at")) < F.lit("2025-03-08").cast("date")) & (F.col("quality").isin("BAD", "OK")))
    .select("quality")
)

result = (
    site_side
    .exceptAll(reading_side)
    .orderBy("zone")
)
