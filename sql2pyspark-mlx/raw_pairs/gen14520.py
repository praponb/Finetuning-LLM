from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_quantity").desc())

result = (
    df_inventory
    .groupBy("store_id")
    .agg(F.sum("quantity").alias("total_quantity"))
    .withColumn("rnk", F.dense_rank().over(w))
    .orderBy(F.col("total_quantity").desc())
)
