from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("qty_times_product_id").desc_nulls_first())
w2 = Window.partitionBy("value").orderBy(F.col("product_id").asc_nulls_first())

flattened = df_carts.select("id", "customer_id", "coupon_codes", F.inline("items"))

exploded = (
    flattened
    .select(
        "id",
        "customer_id",
        "product_id",
        "qty",
        "promo",
        F.explode_outer("coupon_codes").alias("value"),
    )
)

filtered = (
    exploded
    .filter(F.col("promo").isNotNull() & (F.col("customer_id") == 15))
    .select(
        "id",
        "product_id",
        "value",
        (F.col("qty") * F.col("product_id")).alias("qty_times_product_id"),
    )
)

with_windows = (
    filtered
    .withColumn("qty_times_product_id_cume_dist", F.cume_dist().over(w))
    .withColumn("product_id_dense_rank", F.dense_rank().over(w2))
)

daily = (
    with_windows
    .groupBy(
        F.when(F.col("product_id_dense_rank") >= 7, "LARGE").otherwise("SMALL").alias("product_id_dense_rank_tier"),
    )
    .agg(
        F.count(F.when(F.col("value") == "WELCOME5", 1)).alias("welcome5_count"),
        F.max(F.when(F.col("value") == "SPRING10", F.col("qty_times_product_id"))).alias("spring10_max_qty_times_product_id"),
        F.count(F.when(F.col("value") == "VIP", 1)).alias("vip_count"),
    )
)

result = daily.select("product_id_dense_rank_tier", "vip_count")
