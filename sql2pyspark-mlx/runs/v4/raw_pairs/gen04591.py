from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("status_bits", "id")
w3 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(-6, -1)

result = (
    df_device_readings
    .withColumn("cumulative_humidity", F.sum("humidity").over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("status_bits_avg_6_prior", F.avg("status_bits").over(w3))
)
