from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc())
w2 = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc(), "id")
w_ordered = Window.partitionBy("device_code").orderBy(F.col("humidity").desc(), "id")
w4 = Window.orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code")

result = (
    df_device_readings
    .select(
        "*",
        F.percent_rank().over(w).alias("status_bits_percent_rank"),
        F.row_number().over(w2).alias("seq_no"),
        F.ntile(10).over(w_ordered).alias("humidity_bucket"),
        F.sum("status_bits").over(w4).alias("running_status_bits"),
        F.round((F.col("pressure") / F.sum("pressure").over(w5)), 4).alias("pressure_share"),
    )
)
