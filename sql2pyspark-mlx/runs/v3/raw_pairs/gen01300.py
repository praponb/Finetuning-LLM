from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity")

result = (
    df_order_items
    .filter(F.col("quantity") <= 3)
    .select("id", "order_id", "quantity", "product_id", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") == 1)
    .select("id", "order_id", "quantity", "product_id")
)
