from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_device_readings
    .select(
        "*",
        F.corr("status_bits", "humidity").over(w).alias("corr_status_bits_humidity"),
        F.sum("status_bits").over(w2).alias("running_status_bits"),
    )
)
