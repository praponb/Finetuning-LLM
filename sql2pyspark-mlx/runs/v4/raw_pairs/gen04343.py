from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01").cast("date")) & (F.col("pressure") <= 1015))
    .withColumn("status_bits_per_temp_c", F.round((F.col("status_bits") / F.col("temp_c")), 2))
    .withColumn("status_bits_str", F.col("status_bits").cast("string"))
)

prev_events = cleaned.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

base = (
    sessionized
    .filter(F.col("is_new_session") != 0)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "prev_reading_ts",
        "is_new_session",
        "session_id",
        F.hour("prev_reading_ts").alias("prev_reading_ts_hour"),
        (F.col("session_id") * F.col("is_new_session")).alias("session_id_times_is_new_session"),
    )
)

result = (
    base
    .select("id", "device_code", "pressure", "temp_c", "humidity")
    .unpivot(
        ["id", "device_code"],
        ["pressure", "temp_c", "humidity"],
        "metric",
        "reading",
    )
    .filter(F.col("reading").isNotNull())
)
