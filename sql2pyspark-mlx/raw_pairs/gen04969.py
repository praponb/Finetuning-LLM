from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.partitionBy("promo").orderBy(F.col("qty_filled").desc_nulls_last())

exploded = (
    df_carts
    .withColumn("li_item", F.explode("items"))
    .select(
        "id",
        "coupon_codes",
        F.col("li_item.qty"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

lines = (
    exploded
    .select("id", "qty", "price", "promo", F.explode_outer("coupon_codes").alias("tag"))
)

cleaned = (
    lines
    .filter(F.col("promo").isNotNull())
    .withColumn("qty_filled", F.coalesce("qty", F.lit(0)))
)

metrics = (
    cleaned
    .filter(F.col("price") <= 200)
    .select(
        "qty",
        "promo",
        "qty_filled",
        F.avg("price").over(w).alias("promo_avg_price"),
        F.rank().over(w2).alias("qty_filled_rank"),
    )
)

cleaned_carts = (
    metrics
    .filter(F.col("promo").isNotNull())
    .select(
        "qty",
        "promo",
        "promo_avg_price",
        "qty_filled_rank",
        F.least("qty_filled_rank", "qty").alias("least_qty_filled_rank_qty"),
        (F.col("promo_avg_price") / F.nullif("qty_filled", F.lit(0))).alias("promo_avg_price_per_qty_filled"),
    )
)

result = (
    cleaned_carts
    .select(
        "promo",
        "promo_avg_price",
        "qty_filled_rank",
        "promo_avg_price_per_qty_filled",
        F.when(F.col("promo").rlike("^(SPRING10|BUNDLE)$"), "GROUP_B").when(F.col("promo").rlike("^(VIP|WELCOME5)$"), "GROUP_A").otherwise("EXTENDED").alias("promo_zone"),
    )
)
