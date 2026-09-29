from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id")
w3 = Window.partitionBy("order_id").orderBy("unit_price")
w_hist = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id")

result = (
    df_order_items
    .withColumn("last_known_unit_price", F.last("unit_price", ignorenulls=True).over(w))
    .withColumn("next_quantity", F.lead("quantity", 2).over(w2))
    .withColumn("unit_price_dense_rank", F.dense_rank().over(w3))
    .withColumn("cumulative_unit_price", F.sum("unit_price").over(w_hist))
    .withColumn("quantity_share", F.round((F.col("quantity") / F.sum("quantity").over(w5)), 4))
)
