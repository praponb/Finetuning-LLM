from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits", "id")
w2 = Window.orderBy("humidity", "id").rowsBetween(-5, -1)
w3 = Window.orderBy("humidity", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.orderBy("humidity", "id")

filtered = (
    df_device_readings
    .filter(F.col("device_code") == "DV-01")
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        (F.col("temp_c") - F.col("humidity")).alias("temp_c_minus_humidity"),
    )
)

deduped = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "humidity")
)

enriched = (
    deduped
    .filter(F.col("humidity") <= 54)
    .withColumn("humidity_avg_5_prior", F.avg("humidity").over(w2))
    .withColumn("humidity_avg_2", F.avg("humidity").over(w3))
    .withColumn("humidity_bucket", F.ntile(3).over(w4))
)

filtered_device_readings = (
    enriched
    .filter(F.col("humidity_avg_2") <= 62)
    .select(
        "*",
        (F.col("humidity_avg_5_prior") - F.col("humidity")).alias("humidity_avg_5_prior_minus_humidity"),
    )
)

result = (
    filtered_device_readings
    .groupBy(
        F.when(F.col("humidity_bucket") >= 8, "LARGE").otherwise("SMALL").alias("humidity_bucket_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum("humidity_avg_5_prior_minus_humidity").alias("total_humidity_avg_5_prior_minus_humidity"),
        F.max("humidity_bucket").alias("max_humidity_bucket"),
        F.percentile_approx("humidity", 0.5).alias("humidity_p50"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("humidity_avg_2", [0.25, 0.5, 0.75]).alias("humidity_avg_2_quartiles"),
    )
)
