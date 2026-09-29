from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").desc(), "id")

flagged = (
    df_device_readings
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w2))

valid = (
    sessionized
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        "is_new_session",
        "session_no",
        F.round((F.col("session_no") / F.col("pressure")), 2).alias("session_no_per_pressure"),
    )
)

valid_device_readings = (
    valid
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 06:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "session_no",
        F.col("session_no_per_pressure").cast("int").alias("session_no_per_pressure_int"),
        F.trim("firmware").alias("clean_firmware"),
    )
)

base = (
    valid_device_readings
    .filter(F.col("humidity") <= 42)
    .select("*", F.trim("device_code").alias("clean_device_code"))
)

ranked = (
    base
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "temp_c", "session_no", "clean_device_code")
)

cleaned = (
    ranked
    .filter(F.col("session_no") < 7)
    .select(
        "*",
        F.when(F.col("temp_c") >= 41.5, "High").when(F.col("temp_c") >= 24, "Medium").otherwise("Low").alias("temp_c_band"),
        F.when(F.col("session_no") >= 6, "High").when(F.col("session_no") >= 4, "Medium").otherwise("Low").alias("session_no_band"),
    )
)

result = (
    cleaned
    .select("id", "temp_c", "session_no", "clean_device_code", "temp_c_band")
    .orderBy(F.col("session_no").asc_nulls_first(), F.col("id").desc())
    .limit(20)
)
