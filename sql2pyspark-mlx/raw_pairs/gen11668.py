from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_device_readings
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .select(
        "id",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "session_start_flag",
        "burst_id",
    )
    .orderBy(F.col("burst_id").desc(), F.col("humidity").desc())
)
