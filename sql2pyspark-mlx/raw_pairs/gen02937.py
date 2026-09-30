from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select("*", F.coalesce("temp_c", F.lit(0)).alias("temp_c_filled"))
)

flagged = (
    valid
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessions_device_readings = flagged.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_device_readings
    .groupBy("device_code")
    .agg(
        F.count_if(F.col("device_code") == "DV-01").alias("dv_01_events"),
        F.count("*").alias("row_count"),
        F.min("reading_ts").alias("first_reading_ts"),
        F.round(F.avg("temp_c"), 2).alias("avg_temp_c"),
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("humidity")).otherwise(0)).alias("dv_02_humidity"),
    )
)
