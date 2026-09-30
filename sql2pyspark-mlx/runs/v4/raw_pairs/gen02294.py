from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("cost_bucket")

result = (
    df_products
    .groupBy(
        F.when(F.col("cost") >= 730, "high").when(F.col("cost") >= 270, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.sum("stock_qty").alias("total_stock_qty"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_stock_qty", F.sum("total_stock_qty").over(w))
    .orderBy("cost_bucket")
)
