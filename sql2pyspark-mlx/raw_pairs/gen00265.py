from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("hour_window").orderBy(F.col("corr_humidity_temp_c").desc())

prev_events = df_device_readings.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

flagged = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        F.sum("is_new_burst").over(w2).alias("burst_id"),
    )
)

result = (
    sessionized
    .groupBy(F.date_trunc("HOUR", "reading_ts").alias("hour_window"), "burst_id")
    .agg(
        F.stddev_samp("pressure").alias("pressure_stddev"),
        F.corr("humidity", "temp_c").alias("corr_humidity_temp_c"),
        F.count(F.when(F.col("device_code") == "DV-03", 1)).alias("dv_03_rows"),
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_count"),
    )
    .filter(F.col("pressure_stddev") >= 0)
    .withColumn("corr_humidity_temp_c_dense_rank", F.dense_rank().over(w3))
)
