from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("pressure").desc(), "id")
w_hist = Window.partitionBy("device_code").orderBy("status_bits_times_temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.to_date("reading_ts") >= "2024-06-01")
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        F.substring("device_code", 1, 2).alias("device_code_prefix"),
    )
)

ranked = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

filtered_device_readings = (
    ranked
    .filter(F.col("humidity") >= 42)
    .withColumn("status_bits_times_temp_c", (F.col("status_bits") * F.col("temp_c")))
)

metrics = (
    filtered_device_readings
    .select(
        "id",
        "device_code",
        "status_bits",
        "device_code_prefix",
        "dedup_rank",
        "status_bits_times_temp_c",
        F.avg("humidity").over(w_hist).alias("running_avg_humidity"),
        F.min("humidity").over(w_hist).alias("running_min_humidity"),
        F.max("humidity").over(w_hist).alias("running_max_humidity"),
    )
)

filtered_device_readings_2 = (
    metrics
    .filter(F.col("status_bits_times_temp_c").between(23, 79))
    .withColumn("clean_device_code", F.trim("device_code"))
    .withColumn("running_avg_humidity_per_dedup_rank", F.round((F.col("running_avg_humidity") / F.col("dedup_rank")), 2))
)

result = (
    filtered_device_readings_2
    .groupBy(
        F.when(F.col("running_max_humidity") >= 66, "LARGE").otherwise("SMALL").alias("running_max_humidity_tier"),
    )
    .agg(
        F.avg(F.when(F.col("device_code") == "DV-02", F.col("running_min_humidity"))).alias("dv_02_avg_running_min_humidity"),
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_rows"),
    )
    .filter(F.col("dv_04_rows") > 1)
)
