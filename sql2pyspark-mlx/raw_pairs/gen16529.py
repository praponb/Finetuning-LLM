from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("device_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device").orderBy(F.col("running_max_device_length").desc(), "id")
w_ordered = Window.partitionBy("customer_id").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device").orderBy("running_max_device_length", "id")
w6 = Window.orderBy("running_max_device_length_bucket", "id")
w7 = Window.partitionBy("country").orderBy("running_min_device_length", "id")
w8 = Window.partitionBy("browser", "customer_id").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w9 = Window.partitionBy("browser").orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_sessions
    .filter((F.col("started_at") > F.to_timestamp(F.lit("2023-03-13 22:00:00"))) & (F.col("device") != "tablet"))
    .withColumn("device_length", F.length("device"))
)

metrics = (
    valid
    .filter(F.col("browser").isin(["Firefox", "Safari", "Chrome"]))
    .withColumn("running_max_device_length", F.max("device_length").over(w))
    .withColumn("running_avg_device_length", F.avg("device_length").over(w))
    .withColumn("running_count_device_length", F.count("device_length").over(w))
    .withColumn("running_min_device_length", F.min("device_length").over(w))
)

deduped = (
    metrics
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

scored = (
    deduped
    .withColumn("running_count_device_length_count_7d", F.count("running_count_device_length").over(w_ordered))
    .withColumn("last_known_browser", F.last("browser", ignorenulls=True).over(w4))
    .withColumn("running_max_device_length_bucket", F.ntile(5).over(w5))
)

enriched = (
    scored
    .withColumn("next_running_max_device_length", F.lead("running_max_device_length", 2).over(w6))
    .withColumn("prev_running_count_device_length_count_7d", F.lag("running_count_device_length_count_7d", 1).over(w7))
)

metrics_sessions = (
    enriched
    .withColumn("running_max_device_length_bucket_count_90d", F.count("running_max_device_length_bucket").over(w8))
    .withColumn("last_known_running_max_device_length", F.last("running_max_device_length", ignorenulls=True).over(w9))
)

result = (
    metrics_sessions
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        "device_length",
        "running_max_device_length",
        "running_count_device_length",
        "running_min_device_length",
        F.when(F.col("browser").rlike("^(Chrome|Edge)$"), "PRIORITY").otherwise("EXTENDED").alias("browser_zone"),
    )
    .orderBy(F.col("device").asc_nulls_first())
)
