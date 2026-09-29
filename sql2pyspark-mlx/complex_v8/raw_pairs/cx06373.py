from pyspark.sql import functions as F

reading_side = (
    df_meter_readings
    .filter(F.col("voltage") <= 238.3)
    .select("id", F.col("quality").alias("label"))
)

reading_side_2 = (
    df_meter_readings
    .filter((~F.col("quality").isin("EST", "OK")) & (F.col("site_id").isNotNull()))
    .select("site_id", "quality")
)

result = (
    reading_side
    .subtract(reading_side_2)
    .orderBy(F.col("label").desc())
)
