from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("is_device").asc_nulls_first())
w2 = Window.partitionBy("attr_key", "customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy(F.col("is_device_percent_rank").desc_nulls_last())
w5 = Window.partitionBy("attr_key").orderBy("is_device_percent_rank")
w6 = Window.partitionBy("customer_id")
w7 = Window.partitionBy("customer_id", "attr_key").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w8 = Window.partitionBy("attr_key", "customer_id").orderBy(F.col("is_device_percent_rank_cume_dist").desc())
w9 = Window.partitionBy("customer_id", "attr_key").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w10 = Window.partitionBy("attr_key")

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "attr_value"),
    )
)

base = (
    flattened
    .filter(F.col("attr_key").isNotNull())
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "attr_key",
        "attr_value",
        F.when(F.col("attr_key") == "device", 1).otherwise(0).alias("is_device"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.percent_rank().over(w).alias("is_device_percent_rank"),
        F.max("is_device").over(w2).alias("is_device_max_1h"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.sum("is_device").over(w3).alias("is_device_sum_1h"),
        F.cume_dist().over(w4).alias("is_device_percent_rank_cume_dist"),
        F.percent_rank().over(w5).alias("is_device_percent_rank_percent_rank"),
    )
)

enriched_carts = (
    enriched
    .withColumn("customer_id_count_is_device_percent_rank_percent_rank", F.count("is_device_percent_rank_percent_rank").over(w6))
    .withColumn("is_device_avg_2h", F.avg("is_device").over(w7))
)

with_windows = (
    enriched_carts
    .select(
        "*",
        F.percent_rank().over(w8).alias("is_device_percent_rank_cume_dist_percent_rank"),
        F.count("is_device_sum_1h").over(w9).alias("is_device_sum_1h_count_1h"),
        F.min("customer_id_count_is_device_percent_rank_percent_rank").over(w10).alias("attr_key_min_customer_id_count_is_device_percent_rank_percent_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "created_at",
        "attr_value",
        "is_device",
        "is_device_percent_rank",
        "is_device_max_1h",
        "is_device_sum_1h",
        "is_device_percent_rank_cume_dist",
        "customer_id_count_is_device_percent_rank_percent_rank",
    )
    .orderBy(F.col("is_device").desc_nulls_last(), F.col("id").desc())
)
