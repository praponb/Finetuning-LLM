from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_device_readings
    .withColumn("new_group_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = with_gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

result = (
    sessions_device_readings
    .withColumn("new_group_flag_tier", F.when(F.col("new_group_flag") >= 1, "HIGH").otherwise("LOW"))
    .withColumn("hour_window", F.date_trunc("hour", "reading_ts"))
    .cube("new_group_flag_tier", "hour_window")
    .agg(
        F.max(F.when(F.col("device_code") == "DV-04", F.col("new_group_flag"))).alias("dv_04_max_new_group_flag"),
        F.round(F.avg("humidity"), 2).alias("avg_humidity"),
        F.min("reading_ts").alias("first_seen"),
        F.max("burst_id").alias("max_burst_id"),
        F.sum("pressure").alias("total_pressure"),
        F.grouping_id().alias("grouping_id"),
    )
)
