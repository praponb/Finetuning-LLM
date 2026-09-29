from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")

result = (
    df_order_items
    .withColumn("max_unit_price_by_order", F.max("unit_price").over(w))
    .filter(F.col("unit_price") == F.col("max_unit_price_by_order"))
    .select("id", "order_id", "unit_price")
    .orderBy(F.col("order_id").desc())
)
