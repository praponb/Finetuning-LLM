from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.orderBy("pressure", "id")
w3 = Window.orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("temp_c", "id")
w5 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("device_code").orderBy("cumulative_status_bits_times_pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_device_readings
    .filter(F.col("temp_c") >= 25)
    .select(
        "*",
        (F.col("status_bits") * F.col("pressure")).alias("status_bits_times_pressure"),
        F.concat_ws("-", "firmware", "device_code").alias("firmware_device_code_key"),
    )
)

scored = (
    base
    .select(
        "*",
        F.avg("status_bits").over(w).alias("status_bits_avg_1h"),
        F.lead("pressure", 1).over(w2).alias("next_pressure"),
        F.sum("status_bits_times_pressure").over(w3).alias("cumulative_status_bits_times_pressure"),
    )
)

deduped = (
    scored
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "firmware_device_code_key",
        "status_bits_avg_1h",
        "next_pressure",
        "cumulative_status_bits_times_pressure",
    )
)

metrics = (
    deduped
    .select(
        "*",
        F.max("temp_c").over(w5).alias("running_max_temp_c"),
        F.count("temp_c").over(w5).alias("running_count_temp_c"),
        F.min("temp_c").over(w5).alias("running_min_temp_c"),
        F.sum("pressure").over(w6).alias("cumulative_pressure"),
    )
)

result = (
    metrics
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        "status_bits_avg_1h",
        "next_pressure",
        F.when(F.col("device_code").rlike("^(DV-04|DV-01)$"), "CORE").otherwise("GROUP_A").alias("device_code_zone"),
    )
    .orderBy(F.col("device_code_zone").desc(), "temp_c")
)
