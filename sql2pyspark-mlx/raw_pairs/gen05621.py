from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_device_readings.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

with_gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

result = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        F.sum("is_new_session").over(w2).alias("island_id"),
    )
)
