from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy(F.col("corr_status_bits_humidity").asc_nulls_first())
w5 = Window.partitionBy("device_code").orderBy(F.col("temp_c_whole_max_1h").asc_nulls_first())

kept_keys = (
    df_device_readings
    .select("id", "device_code")
    .exceptAll(
        df_device_readings
        .filter(F.col("device_code") == "DV-03")
        .select("id", "device_code")
    )
)

reconciled = (
    df_device_readings
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("humidity") < 62)
    .select(
        "*",
        F.lower("firmware").alias("firmware_lower"),
        F.floor("temp_c").alias("temp_c_whole"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.corr("pressure", "temp_c").over(w).alias("corr_pressure_temp_c"),
        F.avg("temp_c").over(w2).alias("temp_c_avg_15m"),
        F.corr("status_bits", "humidity").over(w).alias("corr_status_bits_humidity"),
        F.max("temp_c_whole").over(w3).alias("temp_c_whole_max_1h"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.cume_dist().over(w4).alias("corr_status_bits_humidity_cume_dist"),
        F.dense_rank().over(w5).alias("temp_c_whole_max_1h_dense_rank"),
        F.stddev_samp("temp_c_whole_max_1h").over(w).alias("device_code_stddev_samp_temp_c_whole_max_1h"),
    )
)

result = (
    metrics
    .select(
        "device_code",
        "humidity",
        "pressure",
        "corr_pressure_temp_c",
        "corr_status_bits_humidity",
    )
    .unpivot(
        ["device_code"],
        ["humidity", "pressure", "corr_pressure_temp_c", "corr_status_bits_humidity"],
        "metric",
        "value",
    )
)
