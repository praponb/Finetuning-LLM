from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_device_readings
    .withColumn("session_start_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_device_readings
    .select(
        "id",
        "device_code",
        "reading_ts",
        "pressure",
        "status_bits",
        "firmware",
        "burst_id",
    )
    .orderBy(F.col("pressure").desc(), "reading_ts")
)
