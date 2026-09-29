from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_device_readings.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    sessions_device_readings
    .groupBy(
        F.date_trunc("year", "prev_reading_ts").alias("year_start"),
        F.when(F.col("status_bits") >= 12, "LARGE").otherwise("SMALL").alias("status_bits_tier"),
        F.date_trunc("hour", "reading_ts").alias("hour_window"),
    )
    .agg(
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_rows"),
        F.min("prev_reading_ts").alias("first_prev_reading_ts"),
        F.max(F.when(F.col("device_code") == "DV-04", F.col("is_new_session"))).alias("dv_04_max_is_new_session"),
        F.count_if(F.col("device_code") == "DV-04").alias("dv_04_events"),
    )
)
