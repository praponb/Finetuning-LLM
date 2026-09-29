from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("temp_c", "id")
w2 = Window.partitionBy("device_code").orderBy(F.col("pressure").asc_nulls_first())
w3 = Window.partitionBy("device_code")

result = (
    df_device_readings
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("pressure_dense_rank", F.dense_rank().over(w2))
    .withColumn("corr_status_bits_humidity", F.corr("status_bits", "humidity").over(w3))
)
