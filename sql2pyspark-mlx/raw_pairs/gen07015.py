from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("end_date").desc())
w2 = Window.orderBy("status_length", "id")
w3 = Window.partitionBy(F.date_trunc("MONTH", "end_date")).orderBy(F.col("status_length").desc(), "id")

base = (
    df_subscriptions
    .filter(F.col("start_date") >= F.to_date(F.lit("2023-08-06")))
    .select(
        "*",
        F.length("status").alias("status_length"),
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
    )
)

latest = (
    base
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "end_date", "status_length")
)

metrics = (
    latest
    .select(
        "id",
        "status_length",
        F.ntile(3).over(w2).alias("status_length_bucket"),
        F.ntile(5).over(w3).alias("status_length_bucket_2"),
    )
)

base_subscriptions = (
    metrics
    .filter(F.col("status_length_bucket").between(6, 8))
    .withColumn("status_length_bucket_2_filled", F.coalesce("status_length_bucket_2", F.lit(0)))
)

stats = (
    base_subscriptions
    .withColumn("status_length_bucket_2_filled_tier", F.when(F.col("status_length_bucket_2_filled") >= 65, "PREMIUM").otherwise("STANDARD"))
    .withColumn("status_length_tier", F.when(F.col("status_length") >= 87, "HIGH").otherwise("LOW"))
    .withColumn("status_length_bucket_2_tier", F.when(F.col("status_length_bucket_2") >= 8, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["status_length_bucket_2_filled_tier", "status_length_tier", "status_length_bucket_2_tier"], ["status_length_bucket_2_filled_tier", "status_length_tier"], ["status_length_bucket_2_filled_tier"], []],
        "status_length_bucket_2_filled_tier",
        "status_length_tier",
        "status_length_bucket_2_tier",
    )
    .agg(
        F.percentile_approx("status_length_bucket", 0.9).alias("status_length_bucket_p90"),
        F.percentile_approx("status_length_bucket", [0.25, 0.5, 0.75]).alias("status_length_bucket_quartiles"),
        F.round(F.avg("status_length_bucket_2_filled"), 2).alias("avg_status_length_bucket_2_filled"),
    )
)

result = (
    stats
    .select(
        "status_length_bucket_2_filled_tier",
        "status_length_bucket_quartiles",
        "avg_status_length_bucket_2_filled",
    )
    .orderBy("status_length_bucket_2_filled_tier", "avg_status_length_bucket_2_filled")
)
