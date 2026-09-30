from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("humidity") > 68)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "firmware",
        F.dayofweek("reading_ts").alias("reading_ts_dayofweek"),
    )
)

flagged = (
    filtered
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        F.sum("session_start_flag").over(w2).alias("session_no"),
    )
)

result = (
    islands
    .groupBy("device_code")
    .agg(
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_rows"),
        F.percentile_approx("humidity", [0.25, 0.5, 0.75]).alias("humidity_quartiles"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.round(F.avg("humidity"), 2).alias("avg_humidity"),
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_rows"),
    )
)
