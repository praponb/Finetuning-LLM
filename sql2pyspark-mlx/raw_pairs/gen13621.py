from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("last_restock_quarter")

result = (
    df_inventory
    .filter(F.col("quantity") > 200)
    .groupBy(F.quarter("last_restock").alias("last_restock_quarter"))
    .agg(F.sum("quantity").alias("total_quantity"))
    .withColumn("cumulative_quantity", F.sum("total_quantity").over(w))
)
