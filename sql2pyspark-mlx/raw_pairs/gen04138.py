from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("product_id").asc_nulls_last())
w2 = Window.partitionBy("segment").orderBy("qty")

lines = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "item"))
    .select(
        "id",
        "customer_id",
        (F.col("pos") + 1).alias("position"),
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

kept_keys = (
    lines
    .select("id")
    .intersect(
        df_carts
        .filter(F.size("coupon_codes") >= 1)
        .select("id")
    )
)

kept_rows = (
    lines
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.segment").isin(["Corporate", "Consumer"]))
    .select("src.*", F.col("c.segment"), F.col("c.name"))
)

metrics = (
    with_dims
    .select(
        "id",
        "product_id",
        "qty",
        F.dense_rank().over(w).alias("product_id_dense_rank"),
        F.dense_rank().over(w2).alias("qty_dense_rank"),
    )
)

filtered = (
    metrics
    .filter(F.col("product_id_dense_rank").between(4, 7))
    .select(
        "*",
        F.coalesce("product_id_dense_rank", F.lit(0)).alias("product_id_dense_rank_filled"),
    )
)

result = (
    filtered
    .groupBy(
        "product_id",
        F.when(F.col("qty") >= 5, "LARGE").otherwise("SMALL").alias("qty_tier"),
    )
    .agg(
        F.min("product_id").alias("min_product_id"),
        F.max("product_id_dense_rank_filled").alias("max_product_id_dense_rank_filled"),
        F.round(F.avg("product_id"), 2).alias("avg_product_id"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("product_id").alias("max_product_id"),
    )
    .filter(F.col("min_product_id") < 1000000)
    .orderBy(F.col("distinct_ids").desc_nulls_last())
)
