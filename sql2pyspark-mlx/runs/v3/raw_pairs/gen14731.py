from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")

result = (
    df_order_items
    .filter(F.col("order_id").isin([5, 29, 30]))
    .select("id", "product_id", F.count("*").over(w).alias("product_id_count"))
    .orderBy(F.col("id").desc())
)
