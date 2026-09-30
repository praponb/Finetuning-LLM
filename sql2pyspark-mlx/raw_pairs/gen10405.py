from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("humidity").asc_nulls_last(), "id")

ranked = (
    df_device_readings
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "reading_ts", "temp_c", "pressure", "status_bits")
)

result = (
    ranked
    .select("id", "temp_c", "pressure", "status_bits")
    .orderBy(F.col("status_bits").desc_nulls_first(), "id")
)
