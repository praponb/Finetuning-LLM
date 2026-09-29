from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_device_readings
    .withColumn("session_start_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "status_bits",
        F.sum("session_start_flag").over(w2).alias("island_id"),
    )
)

result = (
    sessions_device_readings
    .groupBy(
        F.when(F.col("island_id") >= 9, "PREMIUM").otherwise("STANDARD").alias("island_id_tier"),
    )
    .agg(
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_count"),
        F.bit_or("status_bits").alias("status_bits_bit_or"),
        F.sum("status_bits").alias("total_status_bits"),
    )
    .filter(F.col("total_status_bits") > 0)
    .orderBy(F.col("status_bits_bit_or").asc_nulls_last())
)
