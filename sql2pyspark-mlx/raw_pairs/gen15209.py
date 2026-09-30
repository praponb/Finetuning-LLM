from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_ordered = Window.partitionBy(F.month("reading_ts"), "device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_running = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w_recent = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

base = (
    df_device_readings
    .filter(F.col("pressure") > 1007)
    .select("*", F.dayofweek("reading_ts").alias("reading_ts_dayofweek"))
)

matched_keys = (
    base
    .select("firmware", "id")
    .intersectAll(
        df_device_readings
        .filter(F.col("pressure") < 1027)
        .select("firmware", "id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .withColumn("temp_c_max_1h", F.max("temp_c").over(w))
    .withColumn("status_bits_sum_1h", F.sum("status_bits").over(w_ordered))
    .withColumn("pressure_count_30m", F.count("pressure").over(w_running))
)

valid = (
    scored
    .filter(F.col("reading_ts") > F.lit("2025-09-01 07:00:00").cast("timestamp"))
    .select(
        "*",
        F.coalesce("temp_c", F.lit(0)).alias("temp_c_filled"),
        (F.col("humidity") - F.col("temp_c_max_1h")).alias("humidity_minus_temp_c_max_1h"),
    )
)

scored_device_readings = (
    valid
    .select(
        "*",
        F.max("reading_ts_dayofweek").over(w_recent).alias("reading_ts_dayofweek_max_2h"),
    )
)

result = (
    scored_device_readings
    .groupBy("device_code")
    .agg(
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_rows"),
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_count"),
        F.sum(F.when(F.col("device_code") == "DV-04", F.col("reading_ts_dayofweek")).otherwise(0)).alias("dv_04_reading_ts_dayofweek"),
    )
)
