from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")

result = (
    df_order_items
    .withColumn("min_quantity_by_order", F.min("quantity").over(w))
    .filter(F.col("quantity") == F.col("min_quantity_by_order"))
    .select("id", "order_id", "quantity")
    .orderBy(F.col("id").desc(), F.col("quantity").desc())
)
