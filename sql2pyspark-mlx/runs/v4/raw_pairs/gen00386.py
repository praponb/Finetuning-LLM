from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 05:00:00").cast("timestamp"))
    .select(
        "*",
        F.coalesce("humidity", F.lit(0)).alias("humidity_filled"),
        (F.col("pressure") + F.col("status_bits")).alias("pressure_plus_status_bits"),
    )
)

gaps = (
    filtered
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

islands = gaps.withColumn("session_id", F.sum("is_new_burst").over(w2))

result = (
    islands
    .groupBy("device_code", F.date_trunc("DAY", "reading_ts").alias("activity_day"))
    .agg(
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_rows"),
        F.count_if(F.col("device_code") == "DV-01").alias("dv_01_events"),
        F.corr("pressure", "status_bits").alias("corr_pressure_status_bits"),
    )
    .orderBy(F.col("corr_pressure_status_bits").desc())
)
