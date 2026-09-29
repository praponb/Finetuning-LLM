from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_device_readings.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

flagged = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "status_bits",
        "firmware",
        F.sum("new_group_flag").over(w2).alias("session_id"),
    )
)

result = (
    sessions_device_readings
    .select(
        "id",
        "device_code",
        "session_id",
        F.when(F.col("device_code").rlike("^(DV-01|DV-04)$"), "GROUP_A").otherwise("CORE").alias("device_code_zone"),
    )
    .orderBy("device_code_zone")
)
