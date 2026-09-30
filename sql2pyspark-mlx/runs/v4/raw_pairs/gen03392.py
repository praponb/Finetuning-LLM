from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("last_restock").desc())
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("product_id").orderBy(F.col("sqft").asc_nulls_first())
w4 = Window.partitionBy("product_id", "store_id")
w5 = Window.partitionBy("store_id")
w6 = Window.orderBy(F.col("sqft_share").desc())
unit_price_by_product_id = df_order_items.groupBy("product_id").agg(F.min("unit_price").alias("min_order_item_unit_price"))

latest = (
    df_inventory
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

joined = (
    latest
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "left")
    .select("e.*", F.col("s.sqft"))
)

remaining_keys = (
    joined
    .select("store_id", "product_id")
    .intersectAll(
        df_inventory
        .filter(F.col("store_id").isin([2, 4, 6]))
        .select("store_id", "product_id")
    )
)

reconciled = (
    joined
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4))
    .withColumn("sqft_dense_rank", F.dense_rank().over(w3))
    .withColumn("product_id_avg_sqft", F.avg("sqft").over(w4))
    .withColumn("store_id_stddev_samp_sqft", F.stddev_samp("sqft").over(w5))
    .withColumn("sqft_share_2", F.round((F.col("sqft") / F.sum("sqft").over(w5)), 4))
)

scored = (
    metrics
    .select(
        "*",
        F.min("product_id_avg_sqft").over(w2).alias("product_id_min_product_id_avg_sqft"),
        F.rank().over(w6).alias("sqft_share_rank"),
    )
)

with_dims = (
    scored
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.sqft").alias("store_sqft"))
)

enriched = (
    with_dims
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "inner")
    .select("a.*", F.col("s.store_name"))
)

stats = (
    enriched
    .groupBy(
        "product_id",
        F.when(F.col("product_id_min_product_id_avg_sqft") >= 16614, "LARGE").otherwise("SMALL").alias("product_id_min_product_id_avg_sqft_tier"),
        "store_id",
    )
    .agg(
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.max("store_sqft").alias("max_store_sqft"),
    )
)

result = (
    stats
    .join(unit_price_by_product_id, on="product_id", how="left")
    .select(
        "product_id",
        "product_id_min_product_id_avg_sqft_tier",
        "store_id",
        "max_store_sqft",
        F.col("min_order_item_unit_price"),
    )
)
