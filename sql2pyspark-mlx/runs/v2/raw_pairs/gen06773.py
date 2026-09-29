from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc())

result = (
    df_inventory
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") <= 5)
    .select("id", "product_id", "last_restock", "quantity")
    .orderBy(F.col("id").desc())
)
