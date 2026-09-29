from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_device_readings
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .groupBy(F.date_trunc("HOUR", "reading_ts").alias("hour_window"), "session_no")
    .agg(
        F.round(F.avg("humidity"), 2).alias("avg_humidity"),
        F.percentile_approx("humidity", [0.25, 0.5, 0.75]).alias("humidity_quartiles"),
        F.min("reading_ts").alias("first_seen"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_count"),
    )
)
