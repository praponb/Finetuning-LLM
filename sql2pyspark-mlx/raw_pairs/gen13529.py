from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code")
w4 = Window.partitionBy("device_code").orderBy(F.col("humidity").desc_nulls_last())
w5 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)

with_gaps = (
    df_device_readings
    .withColumn("new_group_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_id", F.sum("new_group_flag").over(w2))

kept_keys = (
    sessionized
    .select("device_code", "firmware")
    .intersectAll(
        df_device_readings
        .filter(F.col("status_bits").between(5, 8))
        .select("device_code", "firmware")
    )
)

kept_rows = (
    sessionized
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")) & (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter(F.col("temp_c").between(27, 35))
    .select("*", F.round("status_bits", 0).alias("status_bits_rounded"))
)

with_windows = (
    filtered
    .filter(F.hour("reading_ts").between(9, 14))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "firmware",
        "new_group_flag",
        "session_id",
        F.round((F.col("pressure") / F.sum("pressure").over(w3)), 4).alias("pressure_share"),
    )
)

metrics = (
    with_windows
    .select(
        "device_code",
        "reading_ts",
        "humidity",
        "new_group_flag",
        "session_id",
        F.cume_dist().over(w4).alias("humidity_cume_dist"),
        F.min("new_group_flag").over(w3).alias("device_code_min_new_group_flag"),
        F.count("pressure_share").over(w5).alias("pressure_share_count_15m"),
    )
)

kept_keys_2 = (
    metrics
    .select("device_code")
    .intersectAll(
        df_device_readings
        .filter(F.col("temp_c").between(22, 27))
        .select("device_code")
    )
)

kept_rows_device_readings = (
    metrics
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

result = (
    kept_rows_device_readings
    .groupBy(
        F.when(F.col("session_id") >= 6, "HIGH").otherwise("LOW").alias("session_id_tier"),
    )
    .agg(
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
    )
    .orderBy(F.col("num_device_codes").asc_nulls_first())
)
