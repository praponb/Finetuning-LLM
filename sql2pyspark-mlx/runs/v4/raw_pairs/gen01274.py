from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_device_readings.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "status_bits",
        "prev_reading_ts",
        "is_new_burst",
        "island_id",
    )
    .orderBy("is_new_burst")
)
