from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("unit_price_bucket")

result = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .groupBy(
        F.when(F.col("unit_price") >= 440, "high").when(F.col("unit_price") >= 100, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.sum("unit_price").alias("total_unit_price"))
    .withColumn("cumulative_unit_price", F.sum("total_unit_price").over(w))
)
