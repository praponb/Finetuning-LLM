from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_device_readings
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

islands = flagged.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    islands
    .groupBy("device_code", F.date_trunc("DAY", "reading_ts").alias("activity_day"))
    .agg(
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_count"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
        F.count("*").alias("cnt"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes_2"),
        F.sum(F.when(F.col("device_code") == "DV-03", F.col("session_no")).otherwise(0)).alias("dv_03_session_no"),
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_rows"),
    )
    .orderBy(F.col("span_minutes_2").asc_nulls_first())
)
