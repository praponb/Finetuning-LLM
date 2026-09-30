from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code")
w3 = Window.partitionBy("device_code").orderBy("pressure")
w4 = Window.partitionBy("device_code").orderBy("running_min_status_bits", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy("running_avg_status_bits", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.orderBy("temp_c", "id").rowsBetween(-6, Window.currentRow)
w7 = Window.orderBy(F.col("status_bits_avg_3").desc(), "id")

valid = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.when(F.col("device_code") == "DV-04", 1).otherwise(0).alias("is_dv_04"),
        F.length("firmware").alias("firmware_length"),
    )
)

scored = (
    valid
    .filter(F.col("temp_c").between(24, 31))
    .select(
        "*",
        F.max("status_bits").over(w_part).alias("running_max_status_bits"),
        F.min("status_bits").over(w_part).alias("running_min_status_bits"),
        F.avg("status_bits").over(w_part).alias("running_avg_status_bits"),
        F.sum("status_bits").over(w_part).alias("running_sum_status_bits"),
        F.avg("is_dv_04").over(w2).alias("device_code_avg_is_dv_04"),
    )
)

latest = (
    scored
    .withColumn("latest_rank", F.rank().over(w3))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "device_code",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        "is_dv_04",
        "firmware_length",
        "running_min_status_bits",
        "running_avg_status_bits",
    )
)

valid_device_readings = (
    latest
    .filter(F.col("running_avg_status_bits").isNull())
    .select(
        "*",
        (F.col("status_bits") / F.nullif("firmware_length", F.lit(0))).alias("status_bits_per_firmware_length"),
    )
)

enriched = (
    valid_device_readings
    .select(
        "id",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware_length",
        "running_min_status_bits",
        F.max("pressure").over(w4).alias("last3_max_pressure"),
        F.count("pressure").over(w4).alias("last3_count_pressure"),
        F.sum("pressure").over(w4).alias("last3_sum_pressure"),
        F.avg("status_bits").over(w5).alias("status_bits_avg_3"),
    )
)

filtered = (
    enriched
    .filter(F.col("last3_sum_pressure") < 3300)
    .select(
        "id",
        "temp_c",
        "pressure",
        "firmware_length",
        "running_min_status_bits",
        "last3_max_pressure",
        "last3_count_pressure",
        "last3_sum_pressure",
        "status_bits_avg_3",
        F.ceil("status_bits").alias("status_bits_whole"),
    )
)

result = (
    filtered
    .select(
        "id",
        "temp_c",
        "last3_count_pressure",
        "last3_sum_pressure",
        F.count("temp_c").over(w6).alias("last7_count_temp_c"),
        F.avg("temp_c").over(w6).alias("last7_avg_temp_c"),
        F.max("temp_c").over(w6).alias("last7_max_temp_c"),
        F.row_number().over(w7).alias("event_seq"),
    )
)
