from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("product_id")

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 5]))
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_quantity", F.sum("total_quantity").over(w))
    .orderBy(F.col("product_id").desc(), "cnt")
)
