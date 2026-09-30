from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w2 = Window.orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("supplier_id").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_last = (
    df_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.category",
        "s.brand",
        "s.price",
        "s.launch_date",
        "s.tags",
        "s.supplier_id",
        F.col("c.quantity"),
        F.col("c.unit_price").alias("order_item_unit_price"),
    )
)

metrics = (
    with_last
    .withColumn("running_order_item_unit_price", F.sum("order_item_unit_price").over(w2))
    .withColumn("running_price", F.sum("price").over(w3))
)

kept = (
    metrics
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.order_id").isin([3, 21, 27])), "left_semi")
)

eligible = (
    kept
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") < F.col("s.launch_date")), "left_semi")
)

result = (
    eligible
    .select("id", "brand", "tags", "supplier_id", "running_order_item_unit_price")
    .orderBy(F.col("brand").desc())
)
