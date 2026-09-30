from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.orderBy(F.col("pressure").desc_nulls_first())
w3 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_recent = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w5 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w_part = Window.orderBy("pressure")
w7 = Window.orderBy(F.col("temp_c").desc_nulls_first())
w8 = Window.orderBy(F.col("humidity_whole").desc_nulls_first())

valid = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.concat_ws("/", "firmware", "device_code").alias("firmware_device_code_key"),
        F.floor("humidity").alias("humidity_whole"),
    )
)

kept_keys = (
    valid
    .select("device_code", "id")
    .subtract(
        df_device_readings
        .filter(F.col("temp_c") < 31)
        .select("device_code", "id")
    )
)

verified = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter((F.col("reading_ts") >= F.to_timestamp(F.lit("2025-09-01 04:00:00"))) & (F.to_date("reading_ts") >= "2025-04-01"))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "firmware_device_code_key",
        "humidity_whole",
        (F.col("humidity") / F.nullif("status_bits", F.lit(0))).alias("humidity_per_status_bits"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.sum("humidity_whole").over(w).alias("humidity_whole_sum_2h"),
        F.percent_rank().over(w2).alias("pressure_percent_rank"),
        F.avg("temp_c").over(w3).alias("temp_c_avg_1h"),
    )
)

scored = (
    metrics
    .withColumn("temp_c_max_15m", F.max("temp_c").over(w_recent))
    .withColumn("pressure_cume_dist", F.cume_dist().over(w2))
    .withColumn("humidity_per_status_bits_max_15m", F.max("humidity_per_status_bits").over(w_recent))
    .withColumn("temp_c_sum_30m", F.sum("temp_c").over(w5))
    .withColumn("pressure_percent_rank_2", F.percent_rank().over(w_part))
)

metrics_device_readings = (
    scored
    .withColumn("temp_c_dense_rank", F.dense_rank().over(w7))
    .withColumn("humidity_whole_percent_rank", F.percent_rank().over(w8))
    .withColumn("temp_c_sum_30m_sum_15m", F.sum("temp_c_sum_30m").over(w_recent))
)

cleaned = (
    metrics_device_readings
    .filter(F.col("humidity_whole_percent_rank").between(0.21, 0.76))
    .select(
        "*",
        F.least("pressure_percent_rank_2", "pressure_percent_rank").alias("least_pressure_percent_rank_2_pressure_percent_rank"),
        F.round("humidity_per_status_bits", 0).alias("humidity_per_status_bits_rounded"),
    )
)

result = (
    cleaned
    .cube(
        F.date_trunc("HOUR", "reading_ts").alias("hour_window"),
        F.when(F.col("humidity_per_status_bits") >= 75, "PREMIUM").otherwise("STANDARD").alias("humidity_per_status_bits_tier"),
        F.when(F.col("temp_c_max_15m") >= 38, "PREMIUM").otherwise("STANDARD").alias("temp_c_max_15m_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
        F.percentile_approx("humidity_per_status_bits_rounded", [0.25, 0.5, 0.75]).alias("humidity_per_status_bits_rounded_quartiles"),
        F.percentile_approx("pressure_percent_rank_2", [0.25, 0.5, 0.75]).alias("pressure_percent_rank_2_quartiles"),
        F.count("*").alias("row_count"),
        F.bit_or("status_bits").alias("status_bits_bit_or"),
        F.max("reading_ts").alias("ended_at"),
    )
)
