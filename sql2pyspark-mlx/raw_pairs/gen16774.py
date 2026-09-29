from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code")
w_ordered = Window.orderBy("humidity", "id")

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") < "2024-09-13")
    .withColumn("temp_c_scaled", F.round((F.col("temp_c") * 0.9), 2))
)

with_windows = (
    cleaned
    .select(
        "id",
        "temp_c",
        "humidity",
        "temp_c_scaled",
        F.sum("temp_c").over(w_grp).alias("running_temp_c"),
        F.round((F.col("pressure") / F.sum("pressure").over(w2)), 4).alias("pressure_share"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "temp_c",
        "pressure_share",
        F.lag("running_temp_c").over(w_ordered).alias("prev_running_temp_c"),
        F.lead("humidity").over(w_ordered).alias("next_humidity"),
        F.lag("humidity").over(w_ordered).alias("prev_humidity"),
        F.row_number().over(w_ordered).alias("event_seq"),
        F.lag("pressure_share", 1).over(w_ordered).alias("prev_pressure_share"),
    )
)

filtered = (
    metrics
    .filter(F.col("next_humidity").between(54, 74))
    .select(
        "id",
        "temp_c",
        "pressure_share",
        "prev_running_temp_c",
        "next_humidity",
        "prev_humidity",
        "prev_pressure_share",
        F.floor("prev_pressure_share").alias("prev_pressure_share_whole"),
    )
)

result = (
    filtered
    .select(
        "id",
        "temp_c",
        "pressure_share",
        "prev_running_temp_c",
        "prev_pressure_share",
        "prev_pressure_share_whole",
    )
    .orderBy("prev_running_temp_c")
)
