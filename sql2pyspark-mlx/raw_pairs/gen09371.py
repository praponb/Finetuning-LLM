from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("device_code").orderBy("humidity_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w_ordered = Window.partitionBy("device_code").orderBy("humidity", "id")
w_running = Window.partitionBy("device_code").orderBy("running_avg_pressure", "id")
w6 = Window.partitionBy("device_code").orderBy("running_count_pressure", "id")
w_running_2 = Window.partitionBy("device_code").orderBy("running_count_pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01 07:00:00").cast("timestamp")) & (F.col("humidity") >= 68))
    .withColumn("humidity_int", F.col("humidity").cast("int"))
)

scored = (
    base
    .withColumn("running_count_pressure", F.count("pressure").over(w_grp))
    .withColumn("running_sum_pressure", F.sum("pressure").over(w_grp))
    .withColumn("running_avg_pressure", F.avg("pressure").over(w_grp))
    .withColumn("running_max_pressure", F.max("pressure").over(w_grp))
    .withColumn("cumulative_humidity_int", F.sum("humidity_int").over(w2))
)

base_device_readings = (
    scored
    .filter(F.col("status_bits").between(4, 8))
    .withColumn("running_sum_pressure_plus_pressure", (F.col("running_sum_pressure") + F.col("pressure")))
)

with_gaps = (
    base_device_readings
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w3) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "running_count_pressure",
        "running_avg_pressure",
        "cumulative_humidity_int",
        F.sum("is_new_burst").over(w2).alias("burst_id"),
    )
)

enriched = (
    islands
    .filter(F.col("burst_id").between(3, 4))
    .select(
        "id",
        "reading_ts",
        "running_avg_pressure",
        "cumulative_humidity_int",
        "burst_id",
        F.ntile(10).over(w_ordered).alias("humidity_bucket"),
        F.ntile(10).over(w_running).alias("running_avg_pressure_bucket"),
        F.lag("running_avg_pressure", 2, 0).over(w6).alias("prev_running_avg_pressure"),
        F.sum("humidity").over(w_running_2).alias("running_humidity"),
    )
)

result = (
    enriched
    .groupBy(
        F.date_trunc("HOUR", "reading_ts").alias("hour_window"),
        "burst_id",
        F.when(F.col("prev_running_avg_pressure") >= 1028, "LARGE").otherwise("SMALL").alias("prev_running_avg_pressure_tier"),
    )
    .agg(
        F.round(F.avg("humidity_bucket"), 2).alias("avg_humidity_bucket"),
        F.min("reading_ts").alias("first_reading_ts"),
        F.round(F.avg("burst_id"), 2).alias("avg_burst_id"),
    )
)
