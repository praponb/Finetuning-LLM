from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("element", "promo")
w2 = Window.partitionBy("promo")
w_running = Window.orderBy(F.col("price").desc_nulls_last())
w_ordered = Window.orderBy(F.col("qty_share").desc_nulls_first())
w5 = Window.partitionBy("element").orderBy("qty_share_cume_dist")

flattened = (
    df_carts
    .withColumn("item", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

lines = (
    flattened
    .select("qty", "price", "promo", F.explode_outer("coupon_codes").alias("element"))
)

metrics = lines.withColumn("qty_share", F.round((F.col("qty") / F.sum("qty").over(w)), 4))

scored = (
    metrics
    .select(
        "*",
        F.round((F.col("qty") / F.sum("qty").over(w2)), 4).alias("qty_share_2"),
        F.percent_rank().over(w_running).alias("price_percent_rank"),
        F.cume_dist().over(w_ordered).alias("qty_share_cume_dist"),
    )
)

scored_carts = (
    scored
    .filter(F.col("element").isin(["WELCOME5", "SPRING10"]))
    .withColumn("qty_share_cume_dist_cume_dist", F.cume_dist().over(w5))
)

result = (
    scored_carts
    .select(
        "qty",
        "price",
        "promo",
        "qty_share",
        "price_percent_rank",
        "qty_share_cume_dist",
        "qty_share_cume_dist_cume_dist",
        F.when(F.col("promo").rlike("^(SPRING10|VIP)$"), "GROUP_A").when(F.col("promo").rlike("^(BUNDLE|WELCOME5)$"), "GROUP_B").otherwise("PRIORITY").alias("promo_zone"),
    )
)
