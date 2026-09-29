from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

base = (
    df_device_readings
    .filter(F.col("reading_ts") > F.to_date(F.lit("2025-09-01")))
    .select(
        "id",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        F.round((F.col("status_bits") / F.col("temp_c")), 2).alias("status_bits_per_temp_c"),
        (F.col("humidity") - F.col("status_bits")).alias("humidity_minus_status_bits"),
    )
)

filtered = (
    base
    .filter((F.col("reading_ts") > F.to_timestamp(F.lit("2025-09-01 02:00:00"))) & (F.col("status_bits_per_temp_c") > 64))
    .select("*", F.weekofyear("reading_ts").alias("reading_ts_weekofyear"))
)

cleaned = (
    filtered
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "humidity_minus_status_bits",
        F.quarter("reading_ts").alias("reading_ts_quarter"),
        F.coalesce("reading_ts_weekofyear", F.lit(0)).alias("reading_ts_weekofyear_filled"),
    )
)

enriched = (
    cleaned
    .filter(F.col("reading_ts_weekofyear_filled").between(35, 36))
    .select(
        "id",
        "humidity",
        "reading_ts_weekofyear_filled",
        F.count("reading_ts_weekofyear_filled").over(w).alias("reading_ts_weekofyear_filled_count_1h"),
        F.avg("status_bits").over(w2).alias("status_bits_avg_15m"),
        F.count("pressure").over(w3).alias("pressure_count_2h"),
    )
)

filtered_device_readings = (
    enriched
    .filter(F.col("reading_ts_weekofyear_filled") > 21)
    .select(
        "*",
        F.round((F.col("reading_ts_weekofyear_filled_count_1h") / F.col("humidity")), 2).alias("reading_ts_weekofyear_filled_count_1h_per_humidity"),
    )
)

valid = (
    filtered_device_readings
    .filter(F.col("pressure_count_2h") <= 10)
    .select(
        "id",
        "humidity",
        "reading_ts_weekofyear_filled",
        "reading_ts_weekofyear_filled_count_1h",
        "status_bits_avg_15m",
        (F.col("reading_ts_weekofyear_filled_count_1h") * F.col("status_bits_avg_15m")).alias("reading_ts_weekofyear_filled_count_1h_times_status_bits_avg_15m"),
    )
)

result = (
    valid
    .select(
        "id",
        "humidity",
        "reading_ts_weekofyear_filled",
        "reading_ts_weekofyear_filled_count_1h",
        "status_bits_avg_15m",
        "reading_ts_weekofyear_filled_count_1h_times_status_bits_avg_15m",
    )
)
