from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_quantity").desc())

result = (
    df_inventory
    .filter(F.col("store_id") == 2)
    .groupBy("product_id")
    .agg(F.sum("quantity").alias("total_quantity"))
    .withColumn("quantity_rank", F.dense_rank().over(w))
    .orderBy("quantity_rank", "product_id")
    .limit(5)
)
