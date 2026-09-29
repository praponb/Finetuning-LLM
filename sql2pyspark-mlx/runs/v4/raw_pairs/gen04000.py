from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id")
w2 = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(-6, -1)

scored = (
    df_order_items
    .withColumn("order_id_min_quantity", F.min("quantity").over(w))
    .withColumn("unit_price_avg_6_prior", F.avg("unit_price").over(w2))
)

result = (
    scored
    .select("id", "order_id", "product_id", "unit_price", "unit_price_avg_6_prior")
)
