from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.orderBy(F.col("qty").desc())
w3 = Window.partitionBy("promo").orderBy("qty")

lines = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select(
        "id",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

with_windows = lines.withColumn("promo_count_product_id", F.count("product_id").over(w))

base = (
    with_windows
    .filter(F.col("promo").isNotNull() & (~F.col("promo").eqNullSafe(F.lit("BUNDLE"))))
    .select(
        "product_id",
        "qty",
        "price",
        "promo",
        "promo_count_product_id",
        (F.col("qty") - F.col("promo_count_product_id")).alias("qty_minus_promo_count_product_id"),
    )
)

metrics = (
    base
    .select(
        "product_id",
        "price",
        "promo",
        "qty_minus_promo_count_product_id",
        F.cume_dist().over(w2).alias("qty_cume_dist"),
        F.round((F.col("product_id") / F.sum("product_id").over(w)), 4).alias("product_id_share"),
        F.cume_dist().over(w3).alias("qty_cume_dist_2"),
    )
)

valid = (
    metrics
    .filter(F.col("promo").isNotNull())
    .select(
        "price",
        "promo",
        "qty_cume_dist",
        "product_id_share",
        "qty_cume_dist_2",
        F.round((F.col("product_id_share") / F.col("product_id")), 2).alias("product_id_share_per_product_id"),
        F.upper("promo").alias("promo_upper"),
        F.coalesce("promo", F.lit("OTHER")).alias("promo_clean"),
    )
)

result = (
    valid
    .select(
        "price",
        "qty_cume_dist",
        "product_id_share",
        "qty_cume_dist_2",
        "product_id_share_per_product_id",
        "promo_upper",
        "promo_clean",
    )
)
