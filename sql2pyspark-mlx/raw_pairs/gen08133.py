from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_device_readings
    .select(
        "id",
        "pressure",
        "status_bits",
        F.avg("status_bits").over(w).alias("running_avg_status_bits"),
        F.min("status_bits").over(w).alias("running_min_status_bits"),
        F.count("status_bits").over(w).alias("running_count_status_bits"),
    )
)
