from pyspark.sql import functions as F

reading_side = (
    df_meter_readings
    .filter(F.col("quality") == "EST")
    .select("id", F.col("site_id").alias("key_id"), F.lit("reading").alias("source"))
)

reading_side_2 = (
    df_meter_readings
    .filter(F.col("quality") == "OK")
    .select("site_id", "id", F.lit("reading_2").alias("source"))
)

result = reading_side.union(reading_side_2)
