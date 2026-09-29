from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(-6, -1)
w3 = Window.orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w5 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("device_code").orderBy("status_bits_avg_6_prior", "id")
w7 = Window.partitionBy(F.year("reading_ts"), "device_code").orderBy(F.col("first_known_pressure").desc(), "id")

valid = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01 04:00:00").cast("timestamp"))
    .withColumn("pressure_whole", F.ceil("pressure"))
)

metrics = (
    valid
    .withColumn("pressure_share", F.round((F.col("pressure") / F.sum("pressure").over(w)), 4))
    .withColumn("status_bits_avg_6_prior", F.avg("status_bits").over(w2))
    .withColumn("first_known_pressure", F.first("pressure", ignorenulls=True).over(w3))
)

gaps = (
    metrics
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w4).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w4) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        "status_bits_avg_6_prior",
        "first_known_pressure",
        F.sum("is_new_session").over(w5).alias("burst_id"),
    )
)

valid_device_readings = (
    sessions_device_readings
    .filter(F.col("burst_id") <= 5)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "status_bits_avg_6_prior",
        "first_known_pressure",
        "burst_id",
        F.hour("reading_ts").alias("reading_ts_hour"),
        (F.col("temp_c") * F.col("burst_id")).alias("temp_c_times_burst_id"),
    )
)

scored = (
    valid_device_readings
    .select(
        "id",
        "reading_ts",
        "status_bits_avg_6_prior",
        "first_known_pressure",
        "burst_id",
        "temp_c_times_burst_id",
        F.lag("first_known_pressure").over(w_hist).alias("prev_first_known_pressure"),
        F.lag("humidity").over(w_hist).alias("prev_humidity"),
        F.row_number().over(w_hist).alias("row_seq"),
        F.ntile(4).over(w7).alias("first_known_pressure_bucket"),
    )
)

result = (
    scored
    .groupBy(
        F.date_trunc("HOUR", "reading_ts").alias("hour_window"),
        F.when(F.col("first_known_pressure_bucket") >= 7, "PREMIUM").otherwise("STANDARD").alias("first_known_pressure_bucket_tier"),
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
    )
    .agg(
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
        F.percentile_approx("prev_humidity", [0.25, 0.5, 0.75]).alias("prev_humidity_quartiles"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes_2"),
    )
    .orderBy("activity_day")
)
