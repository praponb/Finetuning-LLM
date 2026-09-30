from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc())

result = (
    df_inventory
    .filter(F.month("last_restock").isin([4, 9, 12]))
    .select("id", "store_id", "quantity", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") == 1)
    .select("id", "store_id", "quantity")
)
