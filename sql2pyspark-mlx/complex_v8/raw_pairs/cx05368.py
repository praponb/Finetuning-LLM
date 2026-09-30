from pyspark.sql import functions as F

reading_side = (
    df_meter_readings
    .filter(F.col("quality").isNotNull())
    .select(F.col("site_id").alias("ref_id"), F.col("quality").alias("label"))
)

reading_side_2 = (
    df_meter_readings
    .filter(F.col("voltage").between(215.1, 232.4))
    .select("id", "quality")
)

result = (
    reading_side
    .subtract(reading_side_2)
    .orderBy("ref_id")
)
