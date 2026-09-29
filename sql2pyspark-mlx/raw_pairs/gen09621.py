from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("YEAR", "reading_ts"))
w2 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

cleaned = (
    df_device_readings
    .filter(F.col("humidity").between(40, 68))
    .select("*", F.greatest("temp_c", "pressure").alias("greatest_temp_c_pressure"))
)

enriched = (
    cleaned
    .select(
        "id",
        "reading_ts",
        "pressure",
        "status_bits",
        "firmware",
        "greatest_temp_c_pressure",
        F.stddev_samp("temp_c").over(w).alias("reading_ts_period_stddev_samp_temp_c"),
        F.avg("status_bits").over(w2).alias("status_bits_avg_2h"),
    )
)

cleaned_device_readings = (
    enriched
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "reading_ts",
        "status_bits",
        "firmware",
        "greatest_temp_c_pressure",
        "reading_ts_period_stddev_samp_temp_c",
        "status_bits_avg_2h",
        (F.col("reading_ts_period_stddev_samp_temp_c") + F.col("status_bits_avg_2h")).alias("reading_ts_period_stddev_samp_temp_c_plus_status_bits_avg_2h"),
    )
)

valid = (
    cleaned_device_readings
    .filter(F.col("status_bits") < 10)
    .select(
        "id",
        "status_bits",
        "firmware",
        "greatest_temp_c_pressure",
        "reading_ts_period_stddev_samp_temp_c",
        "status_bits_avg_2h",
        F.round("reading_ts_period_stddev_samp_temp_c", 0).alias("reading_ts_period_stddev_samp_temp_c_rounded"),
    )
)

valid_device_readings = (
    valid
    .filter(F.col("reading_ts_period_stddev_samp_temp_c") == 30)
    .select(
        "*",
        F.round((F.col("greatest_temp_c_pressure") * 1.15), 2).alias("greatest_temp_c_pressure_scaled"),
        F.trim("firmware").alias("clean_firmware"),
    )
)

result = (
    valid_device_readings
    .select(
        "id",
        "status_bits",
        "reading_ts_period_stddev_samp_temp_c",
        "reading_ts_period_stddev_samp_temp_c_rounded",
        "greatest_temp_c_pressure_scaled",
    )
    .orderBy(F.col("id").desc_nulls_last(), "reading_ts_period_stddev_samp_temp_c")
)
