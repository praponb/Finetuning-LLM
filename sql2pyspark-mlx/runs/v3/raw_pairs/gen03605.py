from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock")

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3, 5, 6]))
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") <= 3)
    .select("id", "store_id", "last_restock")
)
