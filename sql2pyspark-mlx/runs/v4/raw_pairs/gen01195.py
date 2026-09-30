from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("price").desc())
w_ordered = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)

exploded = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

flattened = (
    exploded
    .select(
        "id",
        "customer_id",
        "created_at",
        "qty",
        "price",
        F.explode("coupon_codes").alias("tag"),
    )
)

with_windows = (
    flattened
    .withColumn("price_percent_rank", F.percent_rank().over(w))
    .withColumn("qty_max_30m", F.max("qty").over(w_ordered))
)

base = (
    with_windows
    .filter((F.col("created_at") >= F.lit("2025-03-01").cast("date")) & (F.col("qty") <= 3))
    .select(
        "*",
        F.greatest("qty_max_30m", "price").alias("greatest_qty_max_30m_price"),
        F.round((F.col("price_percent_rank") / F.col("price")), 2).alias("price_percent_rank_per_price"),
    )
)

result = (
    base
    .select(
        "id",
        "customer_id",
        "price",
        "tag",
        "price_percent_rank",
        "greatest_qty_max_30m_price",
    )
    .orderBy(F.col("price_percent_rank").asc_nulls_last())
)
