from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("product_id")

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.sum("revenue").alias("total_revenue"))
    .withColumn("cumulative_revenue", F.sum("total_revenue").over(w))
    .orderBy(F.col("product_id").desc())
)
