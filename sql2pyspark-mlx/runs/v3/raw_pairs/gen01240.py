from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc())

result = (
    df_inventory
    .filter(F.month("last_restock").isin([5, 8, 12]))
    .withColumn("row_num", F.dense_rank().over(w))
    .filter(F.col("row_num") <= 3)
    .select("id", "store_id", "quantity", "product_id")
)
