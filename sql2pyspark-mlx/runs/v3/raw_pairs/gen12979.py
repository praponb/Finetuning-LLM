from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("product_id")

result = (
    df_inventory
    .filter(F.col("quantity").isNotNull())
    .groupBy("product_id")
    .agg(F.sum("quantity").alias("total_quantity"))
    .withColumn("cumulative_quantity", F.sum("total_quantity").over(w))
    .orderBy(F.col("cumulative_quantity").desc(), F.col("product_id").desc())
)
