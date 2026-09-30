from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock")

result = (
    df_inventory
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") <= 1)
    .select("id", "store_id", "last_restock")
    .orderBy("last_restock")
)
