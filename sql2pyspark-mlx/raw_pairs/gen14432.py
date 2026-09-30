from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("temp_c_tier").orderBy(F.col("dv_01_max_temp_c").desc())

base = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01 01:00:00").cast("timestamp"))
    .withColumn("humidity_filled", F.coalesce("humidity", F.lit(0)))
    .withColumn("reading_ts_period", F.date_trunc("year", "reading_ts"))
)

gaps = (
    base
    .withColumn("session_start_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "firmware",
        F.sum("session_start_flag").over(w2).alias("island_id"),
    )
)

result = (
    sessions_device_readings
    .groupBy(
        F.when(F.col("temp_c") >= 41.5, "LARGE").otherwise("SMALL").alias("temp_c_tier"),
        F.date_trunc("day", "reading_ts").alias("activity_day"),
    )
    .agg(
        F.max(F.when(F.col("device_code") == "DV-01", F.col("temp_c"))).alias("dv_01_max_temp_c"),
        F.min("reading_ts").alias("first_seen"),
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("island_id")).otherwise(0)).alias("dv_02_island_id"),
    )
    .withColumn("dv_01_max_temp_c_rank", F.rank().over(w3))
)
