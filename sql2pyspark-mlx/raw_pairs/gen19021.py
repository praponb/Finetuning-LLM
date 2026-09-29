from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc(), "id")
w2 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w4 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(-6, Window.currentRow)
w_ordered = Window.partitionBy("device_code").orderBy("island_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_device_readings
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

latest = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

gaps = (
    latest
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w3).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w3) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "status_bits",
        "firmware",
        F.sum("session_start_flag").over(w4).alias("session_no"),
    )
)

flagged = (
    islands
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w3).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w3) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

islands_device_readings = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "status_bits",
        F.sum("is_new_burst").over(w4).alias("island_id"),
    )
)

base = (
    islands_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01 09:00:00").cast("timestamp"))
    .select(
        "*",
        (F.col("island_id") * F.col("status_bits")).alias("island_id_times_status_bits"),
    )
)

scored = (
    base
    .filter(F.col("status_bits") > 10)
    .select(
        "*",
        F.count("status_bits").over(w_running).alias("last7_count_status_bits"),
        F.sum("status_bits").over(w_running).alias("last7_sum_status_bits"),
        F.min("status_bits").over(w_running).alias("last7_min_status_bits"),
        F.last("island_id_times_status_bits", ignorenulls=True).over(w_ordered).alias("last_known_island_id_times_status_bits"),
    )
)

kept_keys = (
    scored
    .select("device_code")
    .intersect(
        df_device_readings
        .filter(F.col("pressure") <= 1021)
        .select("device_code")
    )
)

verified = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

result = (
    verified
    .rollup(
        F.when(F.col("island_id_times_status_bits") >= 89, "PREMIUM").otherwise("STANDARD").alias("island_id_times_status_bits_tier"),
    )
    .agg(
        F.bit_or("status_bits").alias("status_bits_bit_or"),
        F.count("*").alias("row_count"),
    )
)
