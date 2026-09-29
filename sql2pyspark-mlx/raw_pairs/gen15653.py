from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code")
w_part = Window.orderBy(F.col("humidity_share").desc())
w5 = Window.orderBy(F.col("corr_status_bits_pressure_times_is_new_session").desc_nulls_last())
w_part_2 = Window.orderBy(F.col("humidity_share").asc_nulls_first())
w_hist = Window.orderBy(F.col("corr_status_bits_pressure_int").desc_nulls_last())

with_gaps = (
    df_device_readings
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = with_gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

remaining_keys = (
    sessions_device_readings
    .select("firmware", "id")
    .intersectAll(
        df_device_readings
        .filter(F.col("reading_ts") >= "2023-08-01")
        .select("firmware", "id")
    )
)

reconciled = (
    sessions_device_readings
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .filter(F.col("humidity") <= 34)
    .select(
        "reading_ts",
        "temp_c",
        "pressure",
        "firmware",
        "is_new_session",
        "burst_id",
        F.round((F.col("humidity") / F.sum("humidity").over(w3)), 4).alias("humidity_share"),
        F.corr("status_bits", "pressure").over(w3).alias("corr_status_bits_pressure"),
    )
)

remaining_keys_2 = (
    with_windows
    .select("firmware")
    .exceptAll(
        df_device_readings
        .filter(F.hour("reading_ts").between(6, 13))
        .select("firmware")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(remaining_keys_2.alias("k"), (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "pressure",
        "firmware",
        "is_new_session",
        "burst_id",
        "humidity_share",
        "corr_status_bits_pressure",
        F.col("corr_status_bits_pressure").cast("int").alias("corr_status_bits_pressure_int"),
    )
)

filtered = (
    base
    .filter(F.col("corr_status_bits_pressure") >= 0.1)
    .select(
        "*",
        (F.col("corr_status_bits_pressure") * F.col("is_new_session")).alias("corr_status_bits_pressure_times_is_new_session"),
    )
)

enriched = (
    filtered
    .select(
        "pressure",
        "firmware",
        "is_new_session",
        "humidity_share",
        "corr_status_bits_pressure_int",
        "corr_status_bits_pressure_times_is_new_session",
        F.dense_rank().over(w_part).alias("humidity_share_dense_rank"),
        F.percent_rank().over(w5).alias("corr_status_bits_pressure_times_is_new_session_percent_rank"),
        F.percent_rank().over(w_part_2).alias("humidity_share_percent_rank"),
        F.cume_dist().over(w_hist).alias("corr_status_bits_pressure_int_cume_dist"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("corr_status_bits_pressure_int_cume_dist") >= 0.69, "HIGH").otherwise("LOW").alias("corr_status_bits_pressure_int_cume_dist_tier"),
        F.when(F.col("humidity_share") >= 0.61, "LARGE").otherwise("SMALL").alias("humidity_share_tier"),
    )
    .agg(
        F.percentile_approx("humidity_share", 0.95).alias("humidity_share_p95"),
        F.count("*").alias("cnt"),
        F.percentile_approx("pressure", 0.5).alias("pressure_p50"),
        F.countDistinct("firmware").alias("distinct_firmwares"),
        F.count("*").alias("cnt_2"),
        F.round(F.avg("humidity_share_dense_rank"), 2).alias("avg_humidity_share_dense_rank"),
    )
    .orderBy(F.col("avg_humidity_share_dense_rank").desc_nulls_last())
)
