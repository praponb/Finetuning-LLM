from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("pressure").desc(), "id")
w_hist = Window.partitionBy("device_code").orderBy("device_code_length", "id")
w3 = Window.orderBy(F.col("humidity").desc_nulls_first())
w4 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_hist_2 = Window.partitionBy("device_code").orderBy("reading_ts", "id")

cleaned = (
    df_device_readings
    .filter(F.col("status_bits") != 10)
    .withColumn("pressure_tier", F.when(F.col("pressure") >= 1025, "High").when(F.col("pressure") >= 997, "Medium").otherwise("Low"))
    .withColumn("device_code_length", F.length("device_code"))
)

metrics = (
    cleaned
    .filter(F.col("pressure") <= 1026)
    .select(
        "*",
        F.ntile(5).over(w).alias("pressure_bucket"),
        F.ntile(3).over(w_hist).alias("device_code_length_bucket"),
        F.cume_dist().over(w3).alias("humidity_cume_dist"),
        F.avg("humidity").over(w4).alias("humidity_avg_1h"),
        F.row_number().over(w_hist_2).alias("event_seq"),
    )
)

kept_keys = (
    metrics
    .select("firmware", "device_code")
    .intersectAll(
        df_device_readings
        .filter(F.col("firmware").isin(["1.1.0", "2.0.1", "1.0.3"]))
        .select("firmware", "device_code")
    )
)

verified = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")) & (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("reading_ts") >= F.to_timestamp(F.lit("2025-09-01 04:00:00")))
    .select("*", F.ceil("humidity").alias("humidity_whole"))
)

result = (
    filtered
    .groupBy(
        F.when(F.col("humidity_whole") >= 80, "HIGH").otherwise("LOW").alias("humidity_whole_tier"),
        "device_code",
    )
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("device_code_length_bucket", [0.25, 0.5, 0.75]).alias("device_code_length_bucket_quartiles"),
        F.sum(F.when(F.col("device_code") == "DV-03", F.col("status_bits")).otherwise(0)).alias("dv_03_status_bits"),
    )
    .filter(F.col("event_count") >= 1)
)
