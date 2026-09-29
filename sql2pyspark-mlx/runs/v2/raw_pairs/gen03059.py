from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_quantity").desc())

result = (
    df_inventory
    .groupBy(F.quarter("last_restock").alias("last_restock_quarter"))
    .agg(F.sum("quantity").alias("total_quantity"))
    .withColumn("quantity_rank", F.dense_rank().over(w))
    .orderBy(F.col("total_quantity").desc())
)
