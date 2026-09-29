from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_device_readings
    .filter(F.col("status_bits") != 10)
    .select("*", F.length("firmware").alias("firmware_length"))
)

with_prev = cleaned.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .withColumn("temp_c_tier", F.when(F.col("temp_c") >= 38.6, "PREMIUM").otherwise("STANDARD"))
    .rollup("temp_c_tier")
    .agg(
        F.corr("temp_c", "status_bits").alias("corr_temp_c_status_bits"),
        F.sum(F.when(F.col("device_code") == "DV-03", F.col("island_id")).otherwise(0)).alias("dv_03_island_id"),
        F.sum(F.when(F.col("device_code") == "DV-01", F.col("firmware_length")).otherwise(0)).alias("dv_01_firmware_length"),
        F.avg(F.when(F.col("device_code") == "DV-04", F.col("temp_c"))).alias("dv_04_avg_temp_c"),
        F.grouping("temp_c_tier").alias("grouping_id"),
    )
    .filter(F.col("dv_04_avg_temp_c") >= 0)
    .orderBy(F.col("dv_01_firmware_length").asc_nulls_first())
)
