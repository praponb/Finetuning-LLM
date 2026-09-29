from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("shipping_fee").desc())

result = (
    df_orders
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") <= 2)
    .select("id", "store_id", "shipping_fee")
    .orderBy(F.col("store_id").desc())
)
