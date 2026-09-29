from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock")

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 6]))
    .select("id", "store_id", "last_restock", F.dense_rank().over(w).alias("dense_rnk"))
)
