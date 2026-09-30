from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("hour_window").orderBy(F.col("span_minutes").desc())

valid = (
    df_device_readings
    .filter(F.col("temp_c") < 23)
    .select("*", F.hour("reading_ts").alias("reading_ts_hour"))
)

flagged = (
    valid
    .withColumn("session_start_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = flagged.withColumn("session_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_device_readings
    .groupBy(F.date_trunc("HOUR", "reading_ts").alias("hour_window"), "device_code")
    .agg(
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
        F.bit_and("status_bits").alias("status_bits_bit_and"),
        F.sum(F.when(F.col("device_code") == "DV-04", F.col("humidity"))).alias("dv_04_total_humidity"),
    )
    .withColumn("span_minutes_dense_rank", F.dense_rank().over(w3))
    .orderBy("device_code")
)
