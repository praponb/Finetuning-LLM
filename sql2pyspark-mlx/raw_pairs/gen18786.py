from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity")

result = (
    df_inventory
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") <= 2)
    .select("id", "product_id", "quantity", "last_restock")
    .orderBy("last_restock")
)
