from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("unit_price_bucket")

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 430, "high").when(F.col("unit_price") >= 80, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.sum("quantity").alias("total_quantity"))
    .withColumn("cumulative_quantity", F.sum("total_quantity").over(w))
    .orderBy("total_quantity")
)
