from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("device_code")
w3 = Window.orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("humidity", "id")
w5 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")
w6 = Window.partitionBy("device_code").orderBy(F.col("status_bits_times_pressure").desc_nulls_first(), "id")

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") < "2024-08-17")
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        (F.col("status_bits") * F.col("pressure")).alias("status_bits_times_pressure"),
        F.when(F.col("firmware") == "2.0.1", 1).otherwise(0).alias("is_2_0_1"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.max("status_bits_times_pressure").over(w).alias("last4_max_status_bits_times_pressure"),
        F.count("status_bits_times_pressure").over(w).alias("last4_count_status_bits_times_pressure"),
        F.avg("status_bits_times_pressure").over(w).alias("last4_avg_status_bits_times_pressure"),
        F.min("status_bits_times_pressure").over(w).alias("last4_min_status_bits_times_pressure"),
        F.stddev_samp("status_bits_times_pressure").over(w2).alias("device_code_stddev_samp_status_bits_times_pressure"),
    )
)

enriched = (
    with_windows
    .filter(F.col("device_code_stddev_samp_status_bits_times_pressure") < 26)
    .withColumn("device_code_stddev_samp_status_bits", F.stddev_samp("status_bits").over(w2))
    .withColumn("device_code_max_last4_min_status_bits_times_pressure", F.max("last4_min_status_bits_times_pressure").over(w2))
    .withColumn("first_known_status_bits_times_pressure", F.first("status_bits_times_pressure", ignorenulls=True).over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("device_code_count_pressure", F.count("pressure").over(w2))
)

deduped = (
    enriched
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

latest = (
    deduped
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .groupBy(
        F.when(F.col("pressure") >= 1025, "LARGE").otherwise("SMALL").alias("pressure_tier"),
        "device_code",
    )
    .agg(
        F.sum(F.when(F.col("device_code") == "DV-03", F.col("device_code_stddev_samp_status_bits_times_pressure"))).alias("dv_03_total_device_code_stddev_samp_status_bits_times_pressure"),
        F.countDistinct("firmware").alias("distinct_firmwares"),
        F.avg("status_bits_times_pressure").alias("avg_status_bits_times_pressure"),
        F.sum(F.when(F.col("device_code") == "DV-01", F.col("last4_max_status_bits_times_pressure")).otherwise(0)).alias("dv_01_last4_max_status_bits_times_pressure"),
        F.avg(F.when(F.col("device_code") == "DV-02", F.col("status_bits_times_pressure"))).alias("dv_02_avg_status_bits_times_pressure"),
    )
)
