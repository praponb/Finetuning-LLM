from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("product_id")

result = (
    df_order_items
    .filter(F.col("quantity") >= 4)
    .groupBy("product_id")
    .agg(F.sum("unit_price").alias("total_unit_price"))
    .withColumn("cumulative_unit_price", F.sum("total_unit_price").over(w))
)
