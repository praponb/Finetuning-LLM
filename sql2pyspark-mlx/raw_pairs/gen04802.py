from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.orderBy("quantity", "id")
w_grp = Window.orderBy(F.col("price").desc())
w4 = Window.orderBy(F.col("price_cume_dist").asc_nulls_first())
w5 = Window.orderBy(F.col("quantity").desc_nulls_first())
w6 = Window.orderBy("price")

filtered = (
    df_products
    .filter(F.col("launch_date") > F.lit("2023-09-20").cast("date"))
    .withColumn("tags_count", F.size("tags"))
)

with_last = (
    filtered
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.category",
        "s.price",
        "s.tags_count",
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

with_latest = (
    with_last
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.price", F.col("c.unit_price").alias("order_item_unit_price"))
)

with_latest_products = (
    with_latest
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.price",
        F.col("c.last_restock").alias("inventory_last_restock"),
        F.col("c.quantity"),
    )
)

enriched = (
    with_latest_products
    .select(
        "*",
        F.lag("price", 2).over(w2).alias("prev_price"),
        F.cume_dist().over(w_grp).alias("price_cume_dist"),
    )
)

remaining_keys = (
    enriched
    .select("id")
    .subtract(
        df_products
        .filter(F.col("cost") <= 680)
        .select("id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched_products = (
    reconciled
    .select(
        "price",
        "inventory_last_restock",
        "prev_price",
        "price_cume_dist",
        F.dense_rank().over(w4).alias("price_cume_dist_dense_rank"),
        F.dense_rank().over(w5).alias("quantity_dense_rank"),
        F.cume_dist().over(w6).alias("price_cume_dist_2"),
    )
)

result = (
    enriched_products
    .select("price", "price_cume_dist_dense_rank", "price_cume_dist_2")
    .orderBy(F.col("price_cume_dist_dense_rank").desc_nulls_first())
)
