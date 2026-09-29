from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("store_id")

result = (
    df_inventory
    .filter(F.month("last_restock").isin([8, 9, 10]))
    .groupBy("store_id")
    .agg(F.sum("quantity").alias("total_quantity"))
    .withColumn("cumulative_quantity", F.sum("total_quantity").over(w))
)
