from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("total_humidity").desc())

flagged = (
    df_device_readings
    .withColumn("new_group_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.sum("new_group_flag").over(w2).alias("burst_id"),
    )
)

result = (
    sessions_device_readings
    .groupBy("device_code", F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        F.sum("humidity").alias("total_humidity"),
        F.min("reading_ts").alias("first_reading_ts"),
        F.round(F.avg("temp_c"), 2).alias("avg_temp_c"),
        F.percentile_approx("pressure", 0.95).alias("pressure_p95"),
    )
    .withColumn("total_humidity_rank", F.rank().over(w3))
)
