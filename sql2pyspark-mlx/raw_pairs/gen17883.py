from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.partitionBy("device_code").orderBy(F.col("adjusted_temp_c_2").desc())
w3 = Window.partitionBy("device_code").orderBy(F.col("device_code_stddev_samp_adjusted_temp_c_2").asc_nulls_last())

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "firmware",
        F.concat("firmware", F.lit(" "), "device_code").alias("firmware_device_code"),
    )
)

matched_keys = (
    filtered
    .select("firmware", "device_code")
    .intersect(
        df_device_readings
        .filter(F.col("pressure").isNotNull())
        .select("firmware", "device_code")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")) & (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("device_code") != "DV-01")
    .select(
        "id",
        "device_code",
        "humidity",
        "firmware",
        "firmware_device_code",
        F.round((F.col("temp_c") * 1.07), 2).alias("adjusted_temp_c"),
        F.round((F.col("temp_c") * 1.07), 2).alias("adjusted_temp_c_2"),
    )
)

scored = (
    base
    .filter(F.col("device_code") != "DV-02")
    .select(
        "id",
        "device_code",
        "firmware",
        F.count("adjusted_temp_c").over(w).alias("device_code_count_adjusted_temp_c"),
        F.avg("adjusted_temp_c").over(w).alias("device_code_avg_adjusted_temp_c"),
        F.percent_rank().over(w2).alias("adjusted_temp_c_2_percent_rank"),
        F.avg("humidity").over(w).alias("device_code_avg_humidity"),
        F.stddev_samp("adjusted_temp_c_2").over(w).alias("device_code_stddev_samp_adjusted_temp_c_2"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "device_code",
        "firmware",
        "adjusted_temp_c_2_percent_rank",
        "device_code_stddev_samp_adjusted_temp_c_2",
        F.max("adjusted_temp_c_2_percent_rank").over(w).alias("device_code_max_adjusted_temp_c_2_percent_rank"),
        F.round((F.col("device_code_avg_humidity") / F.sum("device_code_avg_humidity").over(w)), 4).alias("device_code_avg_humidity_share"),
        F.max("adjusted_temp_c_2_percent_rank").over(w).alias("device_code_max_adjusted_temp_c_2_percent_rank_2"),
        F.rank().over(w3).alias("device_code_stddev_samp_adjusted_temp_c_2_rank"),
        F.max("device_code_stddev_samp_adjusted_temp_c_2").over(w).alias("device_code_max_device_code_stddev_samp_adjusted_temp_c_2"),
    )
)

result = (
    enriched
    .select(
        "id",
        "device_code",
        "firmware",
        "adjusted_temp_c_2_percent_rank",
        "device_code_stddev_samp_adjusted_temp_c_2",
        "device_code_avg_humidity_share",
        "device_code_max_adjusted_temp_c_2_percent_rank_2",
        "device_code_stddev_samp_adjusted_temp_c_2_rank",
    )
    .orderBy("device_code_stddev_samp_adjusted_temp_c_2")
)
