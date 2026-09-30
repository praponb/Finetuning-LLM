from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.orderBy(F.col("humidity").desc_nulls_last())
w3 = Window.orderBy(F.col("status_bits").desc())
w4 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w5 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)

matched_keys = (
    df_device_readings
    .select("firmware", "id")
    .intersectAll(
        df_device_readings
        .filter(F.col("reading_ts") < "2024-11-01")
        .select("firmware", "id")
    )
)

reconciled = (
    df_device_readings
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        F.count("temp_c").over(w_hist).alias("temp_c_count_30m"),
        F.percent_rank().over(w2).alias("humidity_percent_rank"),
    )
)

filtered = (
    metrics
    .filter(F.col("temp_c") > 27)
    .select("*", F.initcap(F.trim("firmware")).alias("firmware_title"))
)

remaining_keys = (
    filtered
    .select("id")
    .intersect(
        df_device_readings
        .filter(F.col("temp_c") < 25)
        .select("id")
    )
)

reconciled_device_readings = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled_device_readings
    .select(
        "*",
        F.percent_rank().over(w3).alias("status_bits_percent_rank"),
        F.max("temp_c_count_30m").over(w4).alias("temp_c_count_30m_max_30m"),
        F.count("humidity_percent_rank").over(w5).alias("humidity_percent_rank_count_1h"),
    )
)
