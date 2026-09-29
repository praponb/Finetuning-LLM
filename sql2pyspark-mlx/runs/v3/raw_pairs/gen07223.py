from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity")

result = (
    df_inventory
    .filter(F.year("last_restock") == 2025)
    .select("id", "store_id", "quantity", F.dense_rank().over(w).alias("dense_rnk"))
    .orderBy("store_id")
)
