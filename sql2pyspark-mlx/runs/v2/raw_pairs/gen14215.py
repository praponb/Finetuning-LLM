from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("price_bucket")

result = (
    df_products
    .groupBy(
        F.when(F.col("price") >= 1230, "high").when(F.col("price") >= 330, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(F.sum("stock_qty").alias("total_stock_qty"))
    .withColumn("cumulative_stock_qty", F.sum("total_stock_qty").over(w))
)
