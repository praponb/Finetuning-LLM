from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_bits", "id")
w2 = Window.partitionBy("device_code")
w3 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_device_readings
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("pressure_share", F.round((F.col("pressure") / F.sum("pressure").over(w2)), 4))
    .withColumn("cumulative_temp_c", F.sum("temp_c").over(w3))
    .withColumn("device_code_min_temp_c", F.min("temp_c").over(w2))
    .withColumn("cumulative_status_bits", F.sum("status_bits").over(w4))
)
