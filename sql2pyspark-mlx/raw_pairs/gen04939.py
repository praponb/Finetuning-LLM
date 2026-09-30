from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price")
w2 = Window.partitionBy("product_id")
w3 = Window.orderBy("quantity", "id")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.min("cost").alias("min_shipment_cost"))

scored = (
    df_order_items
    .withColumn("unit_price_dense_rank", F.dense_rank().over(w))
    .withColumn("quantity_share", F.round((F.col("quantity") / F.sum("quantity").over(w2)), 4))
    .withColumn("prev_quantity", F.lag("quantity", 2, 0).over(w3))
)

result = (
    scored
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "quantity_share",
        "prev_quantity",
        F.col("min_shipment_cost"),
    )
)
