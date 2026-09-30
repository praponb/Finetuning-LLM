from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("status_bits", "id")

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01 07:00:00").cast("timestamp"))
    .select(
        "*",
        F.trim("firmware").alias("clean_firmware"),
        (F.col("status_bits") * F.col("pressure")).alias("status_bits_times_pressure"),
    )
)

flagged = (
    filtered
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        F.sum("session_start_flag").over(w2).alias("burst_id"),
    )
)

gaps = (
    sessions_device_readings
    .withColumn("session_start_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

islands = gaps.withColumn("session_id", F.sum("session_start_flag").over(w2))

ranked = (
    islands
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "status_bits",
        "session_start_flag",
        "session_id",
    )
)

cleaned = (
    ranked
    .filter(F.col("reading_ts") > F.lit("2025-09-01 05:00:00").cast("timestamp"))
    .withColumn("session_id_rounded", F.round("session_id", 1))
    .withColumn("session_start_flag_whole", F.floor("session_start_flag"))
)

result = (
    cleaned
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "session_start_flag",
        "session_id",
        "session_start_flag_whole",
    )
)
