from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_device_readings
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.sum("session_start_flag").over(w2).alias("session_no"),
    )
)

result = (
    sessions_device_readings
    .groupBy("session_no")
    .agg(
        F.bit_or("status_bits").alias("status_bits_bit_or"),
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_rows"),
        F.avg("humidity").alias("avg_humidity"),
        F.sum(F.when(F.col("device_code") == "DV-04", F.col("session_no")).otherwise(0)).alias("dv_04_session_no"),
        F.corr("status_bits", "pressure").alias("corr_status_bits_pressure"),
        F.avg("session_no").alias("avg_session_no"),
    )
    .orderBy("status_bits_bit_or")
)
