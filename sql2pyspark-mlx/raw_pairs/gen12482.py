from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("reading_ts").desc(), "id")
w_part = Window.partitionBy("device_code").orderBy("temp_c_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy(F.col("running_max_pressure").desc(), "id")
w5 = Window.partitionBy("device_code").orderBy("running_count_pressure", "id")
w6 = Window.partitionBy("device_code")
w7 = Window.partitionBy("device_code").orderBy("dedup_rank", "id")
w8 = Window.partitionBy("device_code").orderBy("running_max_pressure", "id")

valid = (
    df_device_readings
    .filter(F.col("temp_c") <= 33)
    .select(
        "*",
        F.length("firmware").alias("firmware_length"),
        F.lower("device_code").alias("device_code_lower"),
    )
)

cleaned = (
    valid
    .filter(F.col("humidity") >= 74)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "pressure",
        "firmware",
        "firmware_length",
        "device_code_lower",
        F.col("temp_c").cast("int").alias("temp_c_int"),
    )
)

latest = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

metrics = (
    latest
    .select(
        "id",
        "device_code",
        "pressure",
        "firmware",
        "firmware_length",
        "device_code_lower",
        F.max("pressure").over(w_part).alias("running_max_pressure"),
        F.count("pressure").over(w_part).alias("running_count_pressure"),
        F.avg("pressure").over(w_part).alias("running_avg_pressure"),
        F.count("firmware_length").over(w3).alias("firmware_length_count_30m"),
    )
)

ranked = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .select(
        "id",
        "device_code",
        "pressure",
        "firmware",
        "firmware_length",
        "device_code_lower",
        "running_max_pressure",
        "running_count_pressure",
        "running_avg_pressure",
        "firmware_length_count_30m",
        "dedup_rank",
        F.lead("running_count_pressure", 1).over(w5).alias("next_running_count_pressure"),
        F.round((F.col("running_max_pressure") / F.sum("running_max_pressure").over(w6)), 4).alias("running_max_pressure_share"),
        F.round((F.col("running_count_pressure") / F.sum("running_count_pressure").over(w6)), 4).alias("running_count_pressure_share"),
        F.row_number().over(w7).alias("event_seq"),
        F.ntile(4).over(w8).alias("running_max_pressure_bucket"),
    )
)
