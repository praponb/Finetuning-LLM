from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("category", "supplier_id")
w3 = Window.partitionBy("brand").orderBy("cost", "id")
w4 = Window.orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("supplier_id", "category")
w6 = Window.partitionBy("brand").orderBy(F.col("cost").asc_nulls_last())

eligible = (
    df_products
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.order_id").isin([3, 16])), "left_anti")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"))
)

scored = (
    latest_match
    .withColumn("price_share", F.round((F.col("price") / F.sum("price").over(w2)), 4))
    .withColumn("prev_stock_qty", F.lag("stock_qty", 2, 0).over(w3))
    .withColumn("cumulative_price", F.sum("price").over(w4))
    .withColumn("supplier_id_min_quantity", F.min("quantity").over(w5))
    .withColumn("cost_dense_rank", F.dense_rank().over(w6))
)

result = (
    scored
    .select("id", "supplier_id", "cost", "price", "price_share")
    .unpivot(
        ["id", "supplier_id"],
        ["cost", "price", "price_share"],
        "measure",
        "metric_value",
    )
)
