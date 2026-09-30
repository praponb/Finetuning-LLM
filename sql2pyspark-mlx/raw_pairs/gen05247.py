from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

with_latest = (
    df_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.category",
        "s.price",
        "s.tags",
        F.col("c.quantity").alias("order_item_quantity"),
    )
)

with_latest_products = (
    with_latest
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock"),
    )
)

with_latest_products_2 = (
    with_latest_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.category",
        "s.price",
        "s.order_item_quantity",
        F.col("c.quantity").alias("order_item_quantity_2"),
    )
)

matched_keys = (
    with_latest_products_2
    .select("id", "category")
    .exceptAll(
        df_products
        .filter(F.col("supplier_id").isin([5, 18, 30, 37]))
        .select("id", "category")
    )
)

verified = (
    with_latest_products_2
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

remaining_keys = (
    verified
    .select("id", "category")
    .intersectAll(
        df_products
        .filter(F.size("tags") > 2)
        .select("id", "category")
    )
)

kept_rows = (
    verified
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        "category",
        F.when(F.col("order_item_quantity") >= 9, "LARGE").otherwise("SMALL").alias("order_item_quantity_tier"),
    )
    .agg(
        F.max("price").alias("max_price"),
        F.countDistinct("category").alias("distinct_categories"),
    )
    .filter(F.col("max_price") >= 0)
    .orderBy(F.col("distinct_categories").desc())
)
