from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code")

result = (
    df_device_readings
    .filter(F.col("reading_ts") >= "2025-03-01")
    .select(
        "*",
        F.avg("pressure").over(w_hist).alias("running_avg_pressure"),
        F.min("pressure").over(w_hist).alias("running_min_pressure"),
        F.count("pressure").over(w_hist).alias("running_count_pressure"),
        F.round((F.col("humidity") / F.sum("humidity").over(w2)), 4).alias("humidity_share"),
    )
)
