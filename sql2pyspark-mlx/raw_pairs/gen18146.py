from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity")

result = (
    df_inventory
    .filter(F.col("last_restock") >= "2023-09-01")
    .select("id", "store_id", "quantity", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 1)
    .select("id", "store_id", "quantity")
)
