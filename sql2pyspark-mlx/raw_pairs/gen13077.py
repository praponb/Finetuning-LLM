from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")
w3 = Window.orderBy("status_bits", "id")

result = (
    df_device_readings
    .select(
        "*",
        F.last("temp_c", ignorenulls=True).over(w).alias("last_known_temp_c"),
        F.row_number().over(w2).alias("event_seq"),
        F.lead("humidity", 1, 0).over(w3).alias("next_humidity"),
    )
)
