from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("activity_day").orderBy(F.col("dv_01_count").desc())

base = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01 08:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        (F.datediff(F.current_date(), "reading_ts") / 365).alias("years_since_reading_ts"),
        F.weekofyear("reading_ts").alias("reading_ts_weekofyear"),
    )
)

cleaned = (
    base
    .filter((F.col("reading_ts") > F.lit("2025-09-01").cast("date")) & (F.col("pressure") <= 999))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        "years_since_reading_ts",
        F.length("device_code").alias("device_code_length"),
        F.when(F.col("firmware") == "1.0.3", 1).otherwise(0).alias("is_1_0_3"),
    )
)

kept_keys = (
    cleaned
    .select("firmware", "device_code")
    .exceptAll(
        df_device_readings
        .filter(F.col("temp_c").between(29, 38))
        .select("firmware", "device_code")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")) & (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("reading_ts") > F.to_date(F.lit("2025-09-01")))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "firmware",
        "years_since_reading_ts",
        (F.col("humidity") - F.col("device_code_length")).alias("humidity_minus_device_code_length"),
    )
)

result = (
    filtered
    .groupBy(
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
        F.date_trunc("HOUR", "reading_ts").alias("hour_window"),
    )
    .agg(
        F.avg(F.when(F.col("device_code") == "DV-04", F.col("humidity_minus_device_code_length"))).alias("dv_04_avg_humidity_minus_device_code_length"),
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_count"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.count_if(F.col("device_code") == "DV-03").alias("dv_03_events"),
    )
    .withColumn("dv_01_count_rank", F.rank().over(w))
    .orderBy(F.col("dv_01_count").desc_nulls_last())
)
