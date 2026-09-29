from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price")

result = (
    df_order_items
    .filter(F.col("unit_price").between(290, 340))
    .select("id", "product_id", "unit_price", F.dense_rank().over(w).alias("dense_rnk"))
)
