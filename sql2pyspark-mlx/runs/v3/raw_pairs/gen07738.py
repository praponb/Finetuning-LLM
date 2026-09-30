from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc())

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 3, 4]))
    .select("id", "store_id", "quantity", "product_id", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 5)
    .select("id", "store_id", "quantity", "product_id")
)
