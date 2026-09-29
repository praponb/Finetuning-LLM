from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("device_code").orderBy(F.col("island_id_2").desc())
w_running = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("device_code").orderBy("burst_id", "id")

with_gaps = (
    df_device_readings
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

with_gaps_2 = (
    sessionized
    .withColumn("new_group_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

sessionized_device_readings = with_gaps_2.withColumn("island_id", F.sum("new_group_flag").over(w2))

gaps = (
    sessionized_device_readings
    .withColumn("new_group_flag_2", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

islands = gaps.withColumn("island_id_2", F.sum("new_group_flag_2").over(w2))

metrics = (
    islands
    .select(
        "*",
        F.cume_dist().over(w_hist).alias("island_id_2_cume_dist"),
        F.sum("island_id").over(w_running).alias("cumulative_island_id"),
        F.ntile(4).over(w_part).alias("burst_id_bucket"),
        F.ntile(5).over(w_part).alias("burst_id_bucket_2"),
    )
)

result = (
    metrics
    .groupBy("firmware")
    .pivot("device_code", ["DV-01", "DV-03"])
    .agg(F.sum("island_id_2").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "firmware",
        F.col("DV-01_value_sum").alias("dv_01_value_sum"),
        F.col("DV-01_cnt").alias("dv_01_cnt"),
        F.col("DV-03_value_sum").alias("dv_03_value_sum"),
        F.col("DV-03_cnt").alias("dv_03_cnt"),
    )
)
