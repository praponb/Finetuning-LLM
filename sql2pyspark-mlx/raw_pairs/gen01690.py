from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("device_code").orderBy("status_bits_times_pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("device_code").orderBy("status_bits_times_pressure", "id")
w6 = Window.partitionBy("device_code").orderBy("last_known_humidity", "id").rowsBetween(-6, Window.currentRow)
w7 = Window.partitionBy("device_code")

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.to_date(F.lit("2025-09-01")))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        (F.col("status_bits") * F.col("pressure")).alias("status_bits_times_pressure"),
    )
)

gaps = (
    filtered
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

with_windows = (
    sessionized
    .select(
        "id",
        "device_code",
        "reading_ts",
        "status_bits",
        "firmware",
        "is_new_session",
        F.last("humidity", ignorenulls=True).over(w_part).alias("last_known_humidity"),
        F.last("temp_c", ignorenulls=True).over(w4).alias("last_known_temp_c"),
        F.lag("status_bits", 1).over(w_running).alias("prev_status_bits"),
    )
)

valid = (
    with_windows
    .filter(F.col("reading_ts") > F.lit("2025-09-01 03:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "status_bits",
        "firmware",
        "is_new_session",
        "last_known_humidity",
        "prev_status_bits",
        F.round("status_bits", 0).alias("status_bits_rounded"),
    )
)

with_windows_device_readings = (
    valid
    .filter(F.col("is_new_session").between(0, 1))
    .withColumn("last7_avg_prev_status_bits", F.avg("prev_status_bits").over(w6))
    .withColumn("last7_sum_prev_status_bits", F.sum("prev_status_bits").over(w6))
    .withColumn("last7_max_prev_status_bits", F.max("prev_status_bits").over(w6))
    .withColumn("device_code_min_prev_status_bits", F.min("prev_status_bits").over(w7))
)

result = (
    with_windows_device_readings
    .select(
        "id",
        "device_code",
        "last7_sum_prev_status_bits",
        "last_known_humidity",
        "status_bits_rounded",
    )
    .unpivot(
        ids=["id", "device_code"],
        values=["last7_sum_prev_status_bits", "last_known_humidity", "status_bits_rounded"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
)
