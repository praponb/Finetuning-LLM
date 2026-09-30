from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy(F.date_trunc("month", "last_restock"), "product_id").orderBy("quantity", "id")
w4 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
unit_price_by_product_id = df_order_items.groupBy("product_id").agg(F.sum("unit_price").alias("sum_order_item_unit_price"))

metrics = (
    df_inventory
    .select(
        "*",
        F.last("quantity", ignorenulls=True).over(w_running).alias("last_known_quantity"),
        F.sum("quantity").over(w2).alias("running_quantity"),
        F.lag("quantity", 2).over(w_ordered).alias("prev_quantity"),
        F.sum("quantity").over(w4).alias("cumulative_quantity"),
    )
)

result = (
    metrics
    .join(unit_price_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "last_known_quantity",
        "running_quantity",
        "prev_quantity",
        "cumulative_quantity",
        F.col("sum_order_item_unit_price"),
    )
    .orderBy(F.col("prev_quantity").desc(), "store_id")
)
