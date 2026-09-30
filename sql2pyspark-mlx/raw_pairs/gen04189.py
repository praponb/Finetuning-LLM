from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_device_readings
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        F.min("pressure").over(w).alias("last7_min_pressure"),
        F.avg("pressure").over(w).alias("last7_avg_pressure"),
        F.count("pressure").over(w).alias("last7_count_pressure"),
        F.sum("pressure").over(w).alias("last7_sum_pressure"),
        F.sum("pressure").over(w2).alias("cumulative_pressure"),
    )
)
