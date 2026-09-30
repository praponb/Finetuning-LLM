from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("humidity", "id")
w4 = Window.partitionBy("device_code").orderBy("new_group_flag", "id")
w5 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc())
w_hist = Window.partitionBy("device_code").orderBy("pressure")
w_running = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(-3, -1)
w8 = Window.partitionBy("device_code").orderBy("reading_ts_dayofweek", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01 03:00:00").cast("timestamp")) & (F.col("firmware") == "2.0.1"))
    .select("*", F.dayofweek("reading_ts").alias("reading_ts_dayofweek"))
)

with_gaps = (
    cleaned
    .withColumn("new_group_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

latest = (
    islands
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .select(
        "*",
        F.lead("temp_c", 2).over(w4).alias("next_temp_c"),
        F.cume_dist().over(w5).alias("status_bits_cume_dist"),
        F.rank().over(w_hist).alias("pressure_rank"),
        F.avg("temp_c").over(w_running).alias("temp_c_avg_3_prior"),
        F.sum("burst_id").over(w8).alias("running_burst_id"),
    )
)
