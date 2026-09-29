from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy(F.col("humidity").desc(), "id")

valid = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01 04:00:00").cast("timestamp")) & ~F.col("firmware").isin(["1.0.3", "2.0.1"]))
    .select(
        "*",
        F.round((F.col("status_bits") * 1.07), 2).alias("adjusted_status_bits"),
    )
)

result = (
    valid
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        F.avg("status_bits").over(w_part).alias("running_avg_status_bits"),
        F.max("status_bits").over(w_part).alias("running_max_status_bits"),
        F.sum("status_bits").over(w_part).alias("running_sum_status_bits"),
        F.count("status_bits").over(w_part).alias("running_count_status_bits"),
        F.row_number().over(w_hist).alias("seq_no"),
    )
)
