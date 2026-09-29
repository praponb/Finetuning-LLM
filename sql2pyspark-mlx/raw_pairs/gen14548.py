from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").asc_nulls_last())
w2 = Window.partitionBy("order_id")
w_running = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id", "order_id")
unit_price_by_product_id = df_order_items.groupBy("product_id").agg(F.min("unit_price").alias("min_order_item_unit_price"))

scored = (
    df_order_items
    .withColumn("quantity_dense_rank", F.dense_rank().over(w))
    .withColumn("quantity_share", F.round((F.col("quantity") / F.sum("quantity").over(w2)), 4))
    .withColumn("running_unit_price", F.sum("unit_price").over(w_running))
    .withColumn("quantity_share_2", F.round((F.col("quantity") / F.sum("quantity").over(w2)), 4))
    .withColumn("quantity_share_3", F.round((F.col("quantity") / F.sum("quantity").over(w4)), 4))
)

result = (
    scored
    .join(unit_price_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "quantity_dense_rank",
        "quantity_share",
        "running_unit_price",
        "quantity_share_2",
        "quantity_share_3",
        F.col("min_order_item_unit_price"),
    )
    .orderBy(F.col("id").asc_nulls_last())
)
