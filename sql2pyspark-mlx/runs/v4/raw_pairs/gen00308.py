from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_device_readings.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

flagged = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = flagged.withColumn("burst_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_device_readings
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "status_bits",
        "firmware",
        "session_start_flag",
        "burst_id",
        F.when(F.col("device_code").rlike("^(DV-02|DV-03)$"), "GROUP_B").when(F.col("device_code").rlike("^(DV-01|DV-04)$"), "GROUP_A").otherwise("PRIORITY").alias("device_code_zone"),
    )
    .orderBy(F.col("firmware").asc_nulls_last(), "id")
)
