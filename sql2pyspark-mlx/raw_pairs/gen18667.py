from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_device_readings
    .filter(F.hour("reading_ts").between(12, 16))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        F.col("status_bits").cast("string").alias("status_bits_num"),
        F.length("firmware").alias("firmware_length"),
    )
)

with_gaps = (
    cleaned
    .withColumn("new_group_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "status_bits",
        "firmware",
        F.sum("new_group_flag").over(w2).alias("island_id"),
    )
)

result = (
    sessions_device_readings
    .groupBy(F.date_trunc("day", "reading_ts").alias("activity_day"))
    .agg(
        F.percentile_approx("island_id", 0.95).alias("island_id_p95"),
        F.stddev_samp("status_bits").alias("status_bits_stddev"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.max(F.when(F.col("device_code") == "DV-03", F.col("island_id"))).alias("dv_03_max_island_id"),
        F.corr("humidity", "status_bits").alias("corr_humidity_status_bits"),
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("humidity"))).alias("dv_02_total_humidity"),
    )
    .filter(F.col("island_id_p95") < 1000000)
    .orderBy(F.col("island_id_p95").asc_nulls_first())
)
