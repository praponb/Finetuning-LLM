from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("unit_price").desc_nulls_first())
w3 = Window.partitionBy("order_id")
w4 = Window.orderBy(F.col("unit_price").asc_nulls_last())

valid = (
    df_order_items
    .filter(F.col("order_id") == 16)
    .withColumn("unit_price_scaled", F.round((F.col("unit_price") * 1.15), 2))
)

enriched = (
    valid
    .select(
        "*",
        F.sum("unit_price").over(w).alias("running_sum_unit_price"),
        F.avg("unit_price").over(w).alias("running_avg_unit_price"),
        F.count("unit_price").over(w).alias("running_count_unit_price"),
    )
)

kept_keys = (
    enriched
    .select("id")
    .intersect(
        df_order_items
        .filter(F.col("unit_price").between(140, 200))
        .select("id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.discount"), F.col("d.channel"), F.col("d.status"))
)

scored = (
    with_dims
    .filter(F.col("discount") > 0.27)
    .select(
        "id",
        "product_id",
        "quantity",
        "channel",
        F.cume_dist().over(w2).alias("unit_price_cume_dist"),
        F.sum("unit_price").over(w3).alias("order_id_sum_unit_price"),
        F.dense_rank().over(w4).alias("unit_price_dense_rank"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("unit_price_cume_dist") >= 0.66, "PREMIUM").otherwise("STANDARD").alias("unit_price_cume_dist_tier"),
    )
    .agg(
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_rows"),
        F.countDistinct("channel").alias("distinct_channels"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
    )
    .orderBy(F.col("unit_price_cume_dist_tier").asc_nulls_last())
)
