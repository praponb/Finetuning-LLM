from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price")

result = (
    df_order_items
    .filter(F.col("product_id") == 29)
    .select("id", "order_id", "unit_price", "product_id", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") == 1)
    .select("id", "order_id", "unit_price", "product_id")
    .orderBy("id")
)
