from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock")

result = (
    df_inventory
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") <= 2)
    .orderBy("last_restock", F.col("quantity").desc())
)
