from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code")
w4 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)

gaps = (
    df_device_readings
    .withColumn("session_start_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

with_windows = (
    sessionized
    .select(
        "*",
        F.min("status_bits").over(w3).alias("device_code_min_status_bits"),
        F.max("session_start_flag").over(w4).alias("session_start_flag_max_30m"),
        F.avg("status_bits").over(w5).alias("status_bits_avg_6"),
        F.sum("pressure").over(w6).alias("cumulative_pressure"),
        F.max("burst_id").over(w7).alias("burst_id_max_30m"),
    )
)

with_gaps = (
    with_windows
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

by_type = (
    islands
    .groupBy("firmware")
    .pivot("device_code", ["DV-03", "DV-02"])
    .agg(F.avg("temp_c").alias("amount"))
    .select("firmware", F.col("DV-03").alias("dv_03"), F.col("DV-02").alias("dv_02"))
)

result = (
    by_type
    .select("firmware", "dv_03", "dv_02")
    .orderBy(F.col("dv_02").desc(), "firmware")
)
