from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price")

result = (
    df_order_items
    .filter(F.col("unit_price").between(180, 270))
    .select("id", "product_id", "unit_price", "quantity", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") == 1)
    .select("id", "product_id", "unit_price", "quantity")
    .orderBy("unit_price")
)
