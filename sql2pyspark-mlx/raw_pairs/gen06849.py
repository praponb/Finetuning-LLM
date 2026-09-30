from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_device_readings
    .filter(~F.col("firmware").isin(["1.1.0", "2.0.1"]))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        F.upper("device_code").alias("device_code_upper"),
    )
)

with_gaps = (
    valid
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

gaps = (
    sessionized
    .withColumn("new_group_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = gaps.withColumn("session_id", F.sum("new_group_flag").over(w2))

kept_keys = (
    sessions_device_readings
    .select("firmware", "device_code")
    .intersectAll(
        df_device_readings
        .filter(F.col("device_code") != "DV-02")
        .select("firmware", "device_code")
    )
)

kept_rows = (
    sessions_device_readings
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")) & (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

valid_device_readings = (
    kept_rows
    .filter(F.col("reading_ts") >= "2025-02-01")
    .withColumn("greatest_new_group_flag_session_id", F.greatest("new_group_flag", "session_id"))
)

valid_device_readings_2 = (
    valid_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01").cast("date")) & (F.col("status_bits") == 11))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "status_bits",
        "firmware",
        "device_code_upper",
        "is_new_session",
        "greatest_new_group_flag_session_id",
        F.dayofmonth("reading_ts").alias("reading_ts_day"),
        F.floor("session_no").alias("session_no_whole"),
    )
)

base = (
    valid_device_readings_2
    .filter(F.col("reading_ts") > F.lit("2025-09-01 07:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "status_bits",
        "device_code_upper",
        "is_new_session",
        "greatest_new_group_flag_session_id",
        "reading_ts_day",
        "session_no_whole",
        F.upper("device_code").alias("device_code_upper_2"),
    )
)

result = (
    base
    .select(
        "id",
        "reading_ts",
        "device_code_upper",
        "is_new_session",
        "greatest_new_group_flag_session_id",
        "session_no_whole",
        "device_code_upper_2",
    )
    .orderBy(F.col("greatest_new_group_flag_session_id").desc_nulls_first())
)
