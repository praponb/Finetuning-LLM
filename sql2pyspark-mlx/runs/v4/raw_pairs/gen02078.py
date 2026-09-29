from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("last_restock")

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 5, 6]))
    .select("id", "last_restock", F.rank().over(w).alias("rnk"))
)
