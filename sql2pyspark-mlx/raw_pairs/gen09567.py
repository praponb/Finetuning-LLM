from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("humidity", "id")
w_running = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.orderBy("humidity_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("humidity_bucket", "id")
w5 = Window.orderBy(F.col("humidity_bucket").desc_nulls_first())
w6 = Window.orderBy("humidity_bucket_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.to_timestamp(F.lit("2025-09-01 05:00:00")))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "firmware",
        F.round((F.col("humidity") * 1.15), 2).alias("humidity_adjusted"),
        F.round((F.col("humidity") * 1.07), 2).alias("adjusted_humidity"),
    )
)

enriched = (
    base
    .filter(F.col("adjusted_humidity") < 51)
    .select(
        "id",
        "firmware",
        "humidity_adjusted",
        "adjusted_humidity",
        F.ntile(3).over(w).alias("humidity_bucket"),
        F.avg("temp_c").over(w_running).alias("temp_c_avg_30m"),
    )
)

valid = (
    enriched
    .filter(F.col("humidity_bucket").between(3, 7))
    .select(
        "id",
        "firmware",
        "humidity_adjusted",
        "adjusted_humidity",
        "humidity_bucket",
        F.coalesce("humidity_bucket", F.lit(0)).alias("humidity_bucket_filled"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.first("humidity_adjusted", ignorenulls=True).over(w3).alias("first_known_humidity_adjusted"),
        F.lead("adjusted_humidity", 1).over(w4).alias("next_adjusted_humidity"),
        F.rank().over(w5).alias("humidity_bucket_rank"),
        F.sum("humidity_bucket_filled").over(w6).alias("running_humidity_bucket_filled"),
    )
)

valid_device_readings = (
    metrics
    .filter(F.col("next_adjusted_humidity") < 20)
    .withColumn("firmware_group", F.when(F.col("firmware").isin(["1.0.3", "2.0.1"]), "Priority").otherwise("Other"))
)

filtered = (
    valid_device_readings
    .filter(F.col("next_adjusted_humidity") > 74)
    .withColumn("humidity_bucket_rank_per_humidity_bucket", F.round((F.col("humidity_bucket_rank") / F.col("humidity_bucket")), 2))
)

result = (
    filtered
    .groupBy(
        F.when(F.col("running_humidity_bucket_filled") >= 381, "HIGH").otherwise("LOW").alias("running_humidity_bucket_filled_tier"),
    )
    .agg(
        F.round(F.avg("adjusted_humidity"), 2).alias("avg_adjusted_humidity"),
        F.round(F.avg("humidity_bucket_rank_per_humidity_bucket"), 2).alias("avg_humidity_bucket_rank_per_humidity_bucket"),
        F.count("*").alias("num_rows"),
    )
)
