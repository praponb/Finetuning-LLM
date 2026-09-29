from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 4, 5]))
    .select("id", "product_id", "last_restock", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") == 1)
    .select("id", "product_id", "last_restock")
    .orderBy(F.col("id").desc(), F.col("product_id").desc())
)
