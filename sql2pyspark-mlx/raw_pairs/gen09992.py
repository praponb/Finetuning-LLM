from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.partitionBy("device_code")
w4 = Window.partitionBy("device_code").orderBy("humidity", "id")
w5 = Window.orderBy("running_avg_status_bits", "id").rowsBetween(-2, -1)
w6 = Window.orderBy("humidity_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("status_bits").desc())
w8 = Window.orderBy("status_bits_sum_30m", "id").rowsBetween(-6, -1)

base = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 04:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "status_bits",
        "firmware",
        F.col("reading_ts").cast("date").alias("reading_ts_day"),
    )
)

enriched = (
    base
    .withColumn("running_sum_status_bits", F.sum("status_bits").over(w_recent))
    .withColumn("running_count_status_bits", F.count("status_bits").over(w_recent))
    .withColumn("running_avg_status_bits", F.avg("status_bits").over(w_recent))
    .withColumn("running_min_status_bits", F.min("status_bits").over(w_recent))
)

enriched_device_readings = (
    enriched
    .select(
        "id",
        "status_bits",
        "running_avg_status_bits",
        F.sum("status_bits").over(w_ordered).alias("status_bits_sum_30m"),
        F.round((F.col("humidity") / F.sum("humidity").over(w3)), 4).alias("humidity_share"),
        F.ntile(3).over(w4).alias("humidity_bucket"),
    )
)

scored = (
    enriched_device_readings
    .filter(F.col("humidity_share") >= 0.2)
    .select(
        "id",
        "status_bits",
        "running_avg_status_bits",
        "humidity_share",
        F.avg("humidity_share").over(w5).alias("humidity_share_avg_2_prior"),
        F.last("humidity_bucket", ignorenulls=True).over(w6).alias("last_known_humidity_bucket"),
        F.cume_dist().over(w7).alias("status_bits_cume_dist"),
        F.avg("running_avg_status_bits").over(w8).alias("running_avg_status_bits_avg_6_prior"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("running_avg_status_bits_avg_6_prior") >= 11, "PREMIUM").otherwise("STANDARD").alias("running_avg_status_bits_avg_6_prior_tier"),
    )
    .agg(
        F.percentile_approx("humidity_share_avg_2_prior", [0.25, 0.5, 0.75]).alias("humidity_share_avg_2_prior_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("humidity_share"), 2).alias("avg_humidity_share"),
    )
    .orderBy(F.col("distinct_ids").desc_nulls_first())
)
