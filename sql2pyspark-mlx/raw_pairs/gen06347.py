from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("session_no")
w4 = Window.partitionBy("device_code")
w5 = Window.partitionBy("device_code").orderBy(F.col("temp_c").asc_nulls_first())
w_grp = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w7 = Window.partitionBy("device_code").orderBy("session_no")

filtered = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01 01:00:00").cast("timestamp")) & F.col("firmware").isin(["2.0.1", "1.0.3"]))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        (F.col("status_bits") * F.col("pressure")).alias("status_bits_times_pressure"),
    )
)

with_gaps = (
    filtered
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "status_bits",
        "firmware",
        F.sum("is_new_burst").over(w2).alias("session_no"),
    )
)

kept_keys = (
    islands
    .select("device_code", "firmware")
    .subtract(
        df_device_readings
        .filter(F.col("status_bits") != 6)
        .select("device_code", "firmware")
    )
)

kept_rows = (
    islands
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")) & (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .filter(F.col("device_code") != "DV-02")
    .withColumn("session_no_percent_rank", F.percent_rank().over(w3))
    .withColumn("device_code_count_temp_c", F.count("temp_c").over(w4))
    .withColumn("temp_c_cume_dist", F.cume_dist().over(w5))
)

enriched = (
    scored
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "session_no",
        "session_no_percent_rank",
        "temp_c_cume_dist",
        F.count("session_no").over(w_grp).alias("session_no_count_2h"),
        F.max("device_code_count_temp_c").over(w4).alias("device_code_max_device_code_count_temp_c"),
        F.rank().over(w7).alias("session_no_rank"),
    )
)

kept_keys_2 = (
    enriched
    .select("device_code", "id")
    .exceptAll(
        df_device_readings
        .filter(F.col("reading_ts") >= "2023-10-01")
        .select("device_code", "id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.device_code") == F.col("k.device_code")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered_device_readings = (
    reconciled
    .filter(F.col("session_no_rank") == 8)
    .withColumn("device_code_title", F.initcap(F.trim("device_code")))
    .withColumn("temp_c_cume_dist_minus_session_no_percent_rank", (F.col("temp_c_cume_dist") - F.col("session_no_percent_rank")))
)

stats = (
    filtered_device_readings
    .groupBy(F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        F.percentile_approx("temp_c", [0.25, 0.5, 0.75]).alias("temp_c_quartiles"),
        F.sum(F.when(F.col("device_code") == "DV-04", F.col("session_no"))).alias("dv_04_total_session_no"),
        F.min("reading_ts").alias("started_at"),
    )
    .filter(F.col("dv_04_total_session_no") > 0)
)

result = (
    stats
    .select("hour_window", "temp_c_quartiles")
    .orderBy(F.col("hour_window").desc())
)
