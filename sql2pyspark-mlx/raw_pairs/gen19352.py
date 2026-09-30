from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc())

result = (
    df_inventory
    .filter(F.col("product_id") == 5)
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") <= 1)
)
