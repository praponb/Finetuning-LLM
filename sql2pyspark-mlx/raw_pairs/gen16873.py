from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.partitionBy("customer_id", "tag")
w3 = Window.partitionBy(F.month("created_at"), "customer_id")
w4 = Window.orderBy("created_at_hour")
w5 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w6 = Window.partitionBy("customer_id")
w7 = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w8 = Window.partitionBy(F.to_date("created_at"), "customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)

exploded = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("tag"))
)

base = (
    exploded
    .filter((F.col("created_at") >= F.lit("2025-03-01").cast("date")) & F.col("tag").isin(["BUNDLE", "VIP"]))
    .select("*", F.hour("created_at").alias("created_at_hour"))
)

metrics = (
    base
    .filter(F.col("tag") == "VIP")
    .select(
        "*",
        F.max("created_at_hour").over(w).alias("created_at_hour_max_2h"),
        F.sum("created_at_hour").over(w).alias("created_at_hour_sum_2h"),
        F.max("created_at_hour").over(w2).alias("customer_id_max_created_at_hour"),
        F.stddev_samp("created_at_hour").over(w3).alias("created_at_month_stddev_samp_created_at_hour"),
        F.dense_rank().over(w4).alias("created_at_hour_dense_rank"),
    )
)

base_carts = (
    metrics
    .filter((F.col("created_at_month_stddev_samp_created_at_hour") < 52) & F.col("tag").isNotNull())
    .select(
        "id",
        "customer_id",
        "created_at",
        "created_at_hour",
        "created_at_hour_sum_2h",
        "customer_id_max_created_at_hour",
        "created_at_month_stddev_samp_created_at_hour",
        (F.col("customer_id_max_created_at_hour") / F.nullif("created_at_month_stddev_samp_created_at_hour", F.lit(0))).alias("customer_id_max_created_at_hour_per_created_at_month_stddev_samp_created_at_hour"),
        (F.col("created_at_hour_sum_2h") - F.col("created_at_hour_dense_rank")).alias("created_at_hour_sum_2h_minus_created_at_hour_dense_rank"),
    )
)

metrics_carts = (
    base_carts
    .withColumn("created_at_hour_sum_2h_minus_created_at_hour_dense_rank_count_1h", F.count("created_at_hour_sum_2h_minus_created_at_hour_dense_rank").over(w5))
    .withColumn("customer_id_count_customer_id_max_created_at_hour", F.count("customer_id_max_created_at_hour").over(w6))
    .withColumn("created_at_hour_max_1h", F.max("created_at_hour").over(w7))
    .withColumn("customer_id_stddev_samp_created_at_hour_sum_2h", F.stddev_samp("created_at_hour_sum_2h").over(w6))
    .withColumn("created_at_hour_sum_2h_avg_15m", F.avg("created_at_hour_sum_2h").over(w8))
)

enriched = (
    metrics_carts
    .select(
        "customer_id",
        "created_at_hour",
        "created_at_hour_sum_2h",
        "customer_id_max_created_at_hour",
        "created_at_month_stddev_samp_created_at_hour",
        "customer_id_max_created_at_hour_per_created_at_month_stddev_samp_created_at_hour",
        "created_at_hour_sum_2h_minus_created_at_hour_dense_rank",
        "customer_id_count_customer_id_max_created_at_hour",
        "created_at_hour_max_1h",
        "customer_id_stddev_samp_created_at_hour_sum_2h",
        "created_at_hour_sum_2h_avg_15m",
        F.round((F.col("customer_id_count_customer_id_max_created_at_hour") / F.sum("customer_id_count_customer_id_max_created_at_hour").over(w6)), 4).alias("customer_id_count_customer_id_max_created_at_hour_share"),
        F.sum("customer_id_max_created_at_hour_per_created_at_month_stddev_samp_created_at_hour").over(w7).alias("customer_id_max_created_at_hour_per_created_at_month_stddev_samp_created_at_hour_sum_1h"),
    )
)

summary = (
    enriched
    .groupBy(
        F.when(F.col("created_at_hour") >= 73, "HIGH").otherwise("LOW").alias("created_at_hour_tier"),
    )
    .agg(
        F.avg("created_at_hour_sum_2h").alias("avg_created_at_hour_sum_2h"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
    )
    .filter(F.col("distinct_customer_ids") < 1000)
)

result = (
    summary
    .select("created_at_hour_tier", "avg_created_at_hour_sum_2h")
    .orderBy(F.col("created_at_hour_tier").desc())
)
