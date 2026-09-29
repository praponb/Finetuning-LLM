from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("humidity", "id")
w2 = Window.partitionBy(F.month("reading_ts")).orderBy("reading_ts", "id")
w3 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w4 = Window.orderBy("pressure", "id")
w5 = Window.orderBy(F.col("temp_c_whole").desc(), "id")
w6 = Window.orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("humidity_bucket", "id").rowsBetween(-5, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= "2024-05-17")
    .select(
        "id",
        "reading_ts",
        "humidity",
        "pressure",
        "firmware",
        F.floor("temp_c").alias("temp_c_whole"),
    )
)

scored = (
    filtered
    .withColumn("humidity_bucket", F.ntile(10).over(w))
    .withColumn("next_pressure", F.lead("pressure", 1).over(w2))
    .withColumn("temp_c_whole_sum_30m", F.sum("temp_c_whole").over(w3))
    .withColumn("pressure_bucket", F.ntile(3).over(w4))
    .withColumn("temp_c_whole_bucket", F.ntile(5).over(w5))
)

enriched = (
    scored
    .filter(F.to_date("reading_ts") >= "2025-03-22")
    .withColumn("last_known_temp_c_whole", F.last("temp_c_whole", ignorenulls=True).over(w6))
    .withColumn("humidity_avg_5", F.avg("humidity").over(w7))
)

filtered_device_readings = (
    enriched
    .filter(F.col("last_known_temp_c_whole").between(37, 68))
    .select(
        "*",
        F.coalesce("humidity_bucket", F.lit(0)).alias("humidity_bucket_filled"),
    )
)

summary = (
    filtered_device_readings
    .groupBy(
        F.when(F.col("temp_c_whole_bucket") >= 7, "PREMIUM").otherwise("STANDARD").alias("temp_c_whole_bucket_tier"),
    )
    .agg(
        F.max("reading_ts").alias("last_reading_ts"),
        F.min("reading_ts").alias("started_at"),
    )
)

result = summary.select("temp_c_whole_bucket_tier", "last_reading_ts", "started_at")
