from pyspark.sql import functions as F

reading_side = df_meter_readings.select(F.lit("reading").alias("src"), "id", "read_at")

reading_side_2 = df_meter_readings.select(F.lit("reading_2").alias("src"), "id", "read_at")

result = (
    reading_side
    .union(reading_side_2)
    .distinct()
    .orderBy(F.col("read_at").desc(), "src")
)
