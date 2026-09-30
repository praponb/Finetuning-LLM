from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(-2, Window.currentRow)
w_part = Window.partitionBy("device_code").orderBy("pressure", "id")

filtered = (
    df_device_readings
    .filter(F.col("temp_c").isNotNull())
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        F.round("humidity", 1).alias("humidity_rounded"),
        F.lower("firmware").alias("firmware_lower"),
    )
)

result = (
    filtered
    .select(
        "id",
        "device_code",
        "pressure",
        "firmware_lower",
        F.sum("pressure").over(w_recent).alias("last3_sum_pressure"),
        F.min("pressure").over(w_recent).alias("last3_min_pressure"),
        F.count("pressure").over(w_recent).alias("last3_count_pressure"),
        F.row_number().over(w_part).alias("seq_no"),
    )
)
