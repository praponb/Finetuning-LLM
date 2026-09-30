from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("humidity", "id")

ranked = (
    df_device_readings
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .select("id", "device_code", "temp_c", "status_bits", "firmware", "latest_rank")
    .orderBy(F.col("latest_rank").asc_nulls_last())
)
