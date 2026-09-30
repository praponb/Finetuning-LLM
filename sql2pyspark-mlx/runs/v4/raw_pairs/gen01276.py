from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy(F.col("quantity").asc_nulls_first())
w2 = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("unit_price", "id").rowsBetween(-5, Window.currentRow)

result = (
    df_order_items
    .withColumn("quantity_rank", F.rank().over(w))
    .withColumn("cumulative_unit_price", F.sum("unit_price").over(w2))
    .withColumn("quantity_avg_5", F.avg("quantity").over(w3))
)
