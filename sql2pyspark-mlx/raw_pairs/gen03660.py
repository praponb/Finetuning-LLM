from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("humidity_plus_status_bits", "id")
w2 = Window.partitionBy("device_code").orderBy("temp_c", "id")
w_hist = Window.partitionBy("device_code").orderBy("status_bits", "id")
w4 = Window.partitionBy("prev_pressure_minus_status_bits_tier").orderBy(F.col("avg_years_since_reading_ts").desc())

filtered = (
    df_device_readings
    .filter(F.col("pressure").between(1001, 1027))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.when(F.col("pressure") >= 1030, "High").when(F.col("pressure") >= 1001, "Medium").otherwise("Low").alias("pressure_band"),
    )
)

cleaned = (
    filtered
    .filter(F.col("reading_ts") > F.lit("2025-09-01 08:00:00").cast("timestamp"))
    .select(
        "*",
        F.col("reading_ts").cast("date").alias("reading_ts_day"),
        (F.datediff(F.current_date(), "reading_ts") / 365).alias("years_since_reading_ts"),
    )
)

filtered_device_readings = (
    cleaned
    .filter(F.col("firmware") == "2.0.1")
    .withColumn("pressure_minus_status_bits", (F.col("pressure") - F.col("status_bits")))
    .withColumn("humidity_plus_status_bits", (F.col("humidity") + F.col("status_bits")))
)

with_windows = (
    filtered_device_readings
    .select(
        "id",
        "temp_c",
        "pressure",
        "status_bits",
        "years_since_reading_ts",
        "humidity_plus_status_bits",
        F.row_number().over(w).alias("event_seq"),
        F.lag("pressure_minus_status_bits", 1).over(w2).alias("prev_pressure_minus_status_bits"),
        F.lead("humidity_plus_status_bits", 1, 0).over(w_hist).alias("next_humidity_plus_status_bits"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("prev_pressure_minus_status_bits") >= 90, "PREMIUM").otherwise("STANDARD").alias("prev_pressure_minus_status_bits_tier"),
        F.when(F.col("next_humidity_plus_status_bits") >= 73, "HIGH").otherwise("LOW").alias("next_humidity_plus_status_bits_tier"),
    )
    .agg(
        F.sum("event_seq").alias("total_event_seq"),
        F.round(F.avg("years_since_reading_ts"), 2).alias("avg_years_since_reading_ts"),
    )
    .filter(F.col("total_event_seq") >= 0)
    .withColumn("avg_years_since_reading_ts_rank", F.rank().over(w4))
    .orderBy(F.col("total_event_seq").desc())
)
