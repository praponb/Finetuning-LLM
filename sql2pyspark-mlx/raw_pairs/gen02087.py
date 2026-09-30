from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("last_restock").desc())

ranked = (
    df_inventory
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .select("id", "store_id", "product_id", "quantity", "row_num")
    .orderBy("store_id")
)
