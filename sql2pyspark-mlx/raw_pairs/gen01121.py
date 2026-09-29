from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_device_readings
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .select(
        "id",
        "device_code",
        "temp_c",
        "pressure",
        "status_bits",
        "is_new_session",
        "session_id",
    )
    .orderBy(F.col("device_code").desc())
)
