from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id")
w2 = Window.partitionBy("product_id").orderBy("unit_price", "id")
w3 = Window.partitionBy("product_id", "order_id")

result = (
    df_order_items
    .withColumn("unit_price_bucket", F.ntile(3).over(w))
    .withColumn("prev_quantity", F.lag("quantity", 2, 0).over(w2))
    .withColumn("product_id_min_unit_price", F.min("unit_price").over(w3))
)
