from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("qty").desc_nulls_first())
w2 = Window.partitionBy("promo").orderBy(F.col("price").desc_nulls_last())
w3 = Window.partitionBy("promo")

flattened = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

with_windows = (
    flattened
    .select(
        "*",
        F.dense_rank().over(w).alias("qty_dense_rank"),
        F.cume_dist().over(w2).alias("price_cume_dist"),
        F.stddev_samp("qty").over(w3).alias("promo_stddev_samp_qty"),
    )
)

with_dims = (
    with_windows
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.city"))
)

valid = (
    with_dims
    .filter(F.col("promo").isNotNull())
    .withColumn("promo_stddev_samp_qty_scaled", F.round((F.col("promo_stddev_samp_qty") * 100), 2))
    .withColumn("resolved_city", F.coalesce("city", F.lit("UNKNOWN")))
)

result = (
    valid
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "product_id",
        "qty",
        "price",
        "promo",
        "qty_dense_rank",
        "price_cume_dist",
    )
    .orderBy(F.col("product_id").desc())
)
