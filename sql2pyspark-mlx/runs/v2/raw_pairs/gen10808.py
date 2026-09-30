from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc())

result = (
    df_inventory
    .filter(F.col("product_id").isin([4, 5]))
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") == 1)
    .orderBy(F.col("product_id").desc(), F.col("store_id").desc())
)
