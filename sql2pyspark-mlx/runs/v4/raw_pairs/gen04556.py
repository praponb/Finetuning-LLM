from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("greatest_pressure_status_bits").desc_nulls_last())
w2 = Window.partitionBy("device_code").orderBy("greatest_pressure_status_bits", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_device_readings
    .filter(F.col("humidity") > 66)
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.greatest("pressure", "status_bits").alias("greatest_pressure_status_bits"),
        F.round("temp_c", 2).alias("temp_c_rounded"),
    )
)

result = (
    cleaned
    .select(
        "*",
        F.cume_dist().over(w).alias("greatest_pressure_status_bits_cume_dist"),
        F.avg("humidity").over(w2).alias("humidity_avg_3"),
        F.first("greatest_pressure_status_bits", ignorenulls=True).over(w3).alias("first_known_greatest_pressure_status_bits"),
    )
)
