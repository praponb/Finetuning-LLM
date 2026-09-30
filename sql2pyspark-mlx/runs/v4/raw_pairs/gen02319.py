from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_grp = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.orderBy(F.col("temp_c").desc(), "id")
w4 = Window.partitionBy("device_code")

result = (
    df_device_readings
    .withColumn("status_bits_sum_2h", F.sum("status_bits").over(w))
    .withColumn("pressure_sum_15m", F.sum("pressure").over(w_grp))
    .withColumn("event_seq", F.row_number().over(w3))
    .withColumn("pressure_share", F.round((F.col("pressure") / F.sum("pressure").over(w4)), 4))
)
