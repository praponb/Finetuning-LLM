from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("qty").desc_nulls_last())
w2 = Window.partitionBy("promo")
w3 = Window.partitionBy("promo").orderBy("qty")

exploded = df_carts.select("id", "coupon_codes", F.inline("items"))

base = (
    exploded
    .filter(F.col("qty") >= 3)
    .select(
        "id",
        "coupon_codes",
        "product_id",
        "qty",
        "promo",
        F.least("product_id", "price").alias("least_product_id_price"),
        F.round((F.col("qty") * 1.1), 2).alias("qty_scaled"),
        F.coalesce("promo", F.lit("NONE")).alias("resolved_promo"),
    )
)

valid = (
    base
    .filter(F.col("qty_scaled") > 67)
    .select(
        "*",
        F.lower("promo").alias("promo_lower"),
        F.coalesce("promo", F.lit("NONE")).alias("resolved_promo_2"),
    )
)

with_windows = (
    valid
    .withColumn("qty_percent_rank", F.percent_rank().over(w))
    .withColumn("promo_sum_qty_scaled", F.sum("qty_scaled").over(w2))
    .withColumn("qty_percent_rank_2", F.percent_rank().over(w3))
)

valid_carts = (
    with_windows
    .filter(F.col("promo").isNotNull() & (F.col("promo") == "WELCOME5"))
    .select("*", F.ceil("least_product_id_price").alias("least_product_id_price_whole"))
)

result = (
    valid_carts
    .groupBy("product_id")
    .agg(
        F.sum(F.when(F.col("promo") == "BUNDLE", F.col("qty_percent_rank_2")).otherwise(0)).alias("bundle_qty_percent_rank_2"),
        F.count(F.when(F.col("promo") == "VIP", 1)).alias("vip_rows"),
        F.count(F.when(F.col("promo") == "VIP", 1)).alias("vip_rows_2"),
        F.percentile_approx("least_product_id_price", [0.25, 0.5, 0.75]).alias("least_product_id_price_quartiles"),
        F.count(F.when(F.col("promo") == "VIP", 1)).alias("vip_count"),
    )
    .orderBy(F.col("bundle_qty_percent_rank_2").desc())
)
