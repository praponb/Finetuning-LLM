from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_device_readings
    .select(
        "id",
        "device_code",
        "reading_ts",
        "pressure",
        F.min("pressure").over(w).alias("running_min_pressure"),
        F.max("pressure").over(w).alias("running_max_pressure"),
        F.count("pressure").over(w).alias("running_count_pressure"),
        F.sum("pressure").over(w).alias("running_sum_pressure"),
        F.first("pressure", ignorenulls=True).over(w2).alias("first_known_pressure"),
    )
)
