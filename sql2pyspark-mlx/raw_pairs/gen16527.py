from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("element")
w2 = Window.partitionBy("element").orderBy("price")

flattened = (
    df_carts
    .withColumn("li_item", F.explode("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("li_item.qty"),
        F.col("li_item.price"),
    )
)

lines = (
    flattened
    .select("id", "qty", "price", F.explode_outer("coupon_codes").alias("element"))
)

with_windows = (
    lines
    .select(
        "id",
        "qty",
        F.round((F.col("price") / F.sum("price").over(w)), 4).alias("price_share"),
        F.stddev_samp("price").over(w).alias("element_stddev_samp_price"),
        F.rank().over(w2).alias("price_rank"),
        F.min("price").over(w).alias("element_min_price"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("price_share") > 0.69)
    .select(
        "*",
        F.coalesce("element_stddev_samp_price", F.lit(0)).alias("element_stddev_samp_price_filled"),
    )
)

cleaned_carts = (
    cleaned
    .filter(F.col("qty") == 2)
    .select("*", F.ceil("qty").alias("qty_whole"))
)

result = (
    cleaned_carts
    .select(
        "id",
        "qty",
        "price_share",
        "element_stddev_samp_price",
        "price_rank",
        "element_min_price",
        "element_stddev_samp_price_filled",
        "qty_whole",
    )
    .orderBy(F.col("element_min_price").desc())
)
