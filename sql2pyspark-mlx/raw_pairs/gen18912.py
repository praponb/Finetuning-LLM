from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("product_id_rounded").asc_nulls_last())
w2 = Window.partitionBy("promo").orderBy(F.col("price").desc_nulls_first())
w3 = Window.partitionBy("customer_id")
w4 = Window.partitionBy("customer_id").orderBy(F.col("price_rank").desc_nulls_last())

lines = df_carts.select("id", "customer_id", "coupon_codes", F.inline("items"))

flattened = (
    lines
    .select(
        "customer_id",
        "product_id",
        "price",
        "promo",
        F.explode_outer("coupon_codes").alias("tag"),
    )
)

valid = (
    flattened
    .filter(~F.col("tag").isin(["SPRING10", "WELCOME5"]))
    .select(
        "*",
        F.concat_ws("-", "tag", "promo").alias("tag_promo_key"),
        F.round("product_id", 2).alias("product_id_rounded"),
        F.coalesce("promo", F.lit("OTHER")).alias("promo_filled"),
    )
)

enriched = (
    valid
    .select(
        "customer_id",
        "product_id",
        "price",
        "product_id_rounded",
        F.cume_dist().over(w).alias("product_id_rounded_cume_dist"),
        F.rank().over(w2).alias("price_rank"),
        F.count("product_id_rounded").over(w3).alias("customer_id_count_product_id_rounded"),
    )
)

with_windows = (
    enriched
    .filter(F.col("price_rank").isNull())
    .withColumn("customer_id_min_customer_id_count_product_id_rounded", F.min("customer_id_count_product_id_rounded").over(w3))
    .withColumn("price_rank_percent_rank", F.percent_rank().over(w4))
    .withColumn("customer_id_count_product_id_rounded_share", F.round((F.col("customer_id_count_product_id_rounded") / F.sum("customer_id_count_product_id_rounded").over(w3)), 4))
    .withColumn("product_id_share", F.round((F.col("product_id") / F.sum("product_id").over(w3)), 4))
)

result = (
    with_windows
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .groupBy(F.col("d.segment").alias("segment"))
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("a.product_id_rounded_cume_dist")).alias("max_product_id_rounded_cume_dist"),
        F.sum(F.col("a.price_rank_percent_rank")).alias("total_price_rank_percent_rank"),
    )
)
