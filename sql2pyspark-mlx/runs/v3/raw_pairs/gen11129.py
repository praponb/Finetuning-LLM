from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity")

result = (
    df_inventory
    .filter(F.col("quantity") < 218)
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") <= 3)
    .select("id", "store_id", "quantity", "product_id")
    .orderBy(F.col("store_id").desc(), F.col("product_id").desc())
)
