from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits")
w2 = Window.partitionBy("device_code").orderBy(F.col("humidity").asc_nulls_first())
w3 = Window.partitionBy("device_code")

base = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .withColumn("reading_ts_label", F.date_format("reading_ts", "yyyy-MM"))
)

remaining_keys = (
    base
    .select("device_code")
    .subtract(
        df_device_readings
        .filter(F.hour("reading_ts").between(8, 16))
        .select("device_code")
    )
)

verified = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter(F.col("pressure") > 1004)
    .select("*", F.ceil("status_bits").alias("status_bits_whole"))
)

enriched = (
    cleaned
    .filter(F.col("humidity").isNull())
    .select("*", F.percent_rank().over(w).alias("status_bits_percent_rank"))
)

enriched_device_readings = (
    enriched
    .select(
        "id",
        "temp_c",
        "status_bits",
        "firmware",
        "status_bits_whole",
        "status_bits_percent_rank",
        F.dense_rank().over(w2).alias("humidity_dense_rank"),
        F.stddev_samp("temp_c").over(w3).alias("device_code_stddev_samp_temp_c"),
    )
)

cleaned_device_readings = (
    enriched_device_readings
    .filter(F.col("firmware") == "2.0.1")
    .withColumn("is_1_1_0", F.when(F.col("firmware") == "1.1.0", 1).otherwise(0))
)

result = (
    cleaned_device_readings
    .groupBy(
        F.when(F.col("humidity_dense_rank") >= 7, "PREMIUM").otherwise("STANDARD").alias("humidity_dense_rank_tier"),
    )
    .agg(
        F.percentile_approx("device_code_stddev_samp_temp_c", [0.25, 0.5, 0.75]).alias("device_code_stddev_samp_temp_c_quartiles"),
        F.count("*").alias("cnt"),
    )
)
