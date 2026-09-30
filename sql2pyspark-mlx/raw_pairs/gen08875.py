from pyspark.sql import functions as F

valid = (
    df_device_readings
    .filter(F.col("firmware") != "2.0.1")
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        (F.col("humidity") + F.col("temp_c")).alias("humidity_plus_temp_c"),
    )
)

result = (
    valid
    .groupBy(
        "device_code",
        F.date_trunc("HOUR", "reading_ts").alias("hour_window"),
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
    )
    .agg(
        F.round(F.avg("temp_c"), 2).alias("avg_temp_c"),
        F.round(F.sum("pressure") / F.nullif(F.count(F.when(F.col("device_code") == "DV-02", 1)), F.lit(0)), 2).alias("pressure_per_dv_02"),
        F.stddev_samp("humidity").alias("humidity_stddev"),
        F.round(F.avg("temp_c"), 2).alias("avg_temp_c_2"),
        F.percentile_approx("pressure", [0.25, 0.5, 0.75]).alias("pressure_quartiles"),
    )
    .orderBy(F.col("hour_window").desc())
)
