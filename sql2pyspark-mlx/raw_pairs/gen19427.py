from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("activity_day").orderBy(F.col("corr_pressure_status_bits").desc())

valid = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01 01:00:00").cast("timestamp")) & (F.col("humidity") <= 70))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "pressure",
        "status_bits",
        "firmware",
        F.upper("device_code").alias("device_code_upper"),
        F.concat_ws(",", "device_code", "firmware").alias("device_code_firmware_key"),
    )
)

gaps = (
    valid
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

result = (
    islands
    .groupBy(
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
        F.date_trunc("HOUR", "reading_ts").alias("hour_window"),
    )
    .agg(
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_rows"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.corr("pressure", "status_bits").alias("corr_pressure_status_bits"),
    )
    .filter(F.col("corr_pressure_status_bits") > 0)
    .withColumn("corr_pressure_status_bits_rank", F.rank().over(w3))
    .orderBy(F.col("corr_pressure_status_bits").desc_nulls_last())
)
