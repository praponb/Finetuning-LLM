from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("stock_qty").desc_nulls_last())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())

latest = (
    df_products
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

with_last = (
    latest
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn_2", F.row_number().over(w2))
    .filter(F.col("rn_2") == 1)
    .select(
        "s.*",
        F.col("c.unit_price"),
        F.col("c.quantity").alias("order_item_quantity"),
    )
)

joined = (
    with_last
    .alias("a")
    .join(df_suppliers.alias("s"), F.col("a.supplier_id") == F.col("s.id"), "inner")
    .select(
        "a.id",
        "a.product_name",
        "a.category",
        "a.price",
        "a.cost",
        "a.tags",
        "a.supplier_id",
        "a.order_item_quantity",
        F.col("s.country"),
        F.col("s.city"),
    )
)

with_latest = (
    joined
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.price",
        "s.tags",
        "s.order_item_quantity",
        F.col("c.unit_price"),
        F.col("c.quantity"),
    )
)

not_flagged = (
    with_latest
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.store_id").isin([1, 2, 5, 6])), "left_semi")
)

result = not_flagged.select("id", "category", "tags", "quantity")
