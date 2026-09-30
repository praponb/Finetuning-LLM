from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_unit_price").desc())

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 350, "high").when(F.col("unit_price") >= 70, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.sum("unit_price").alias("total_unit_price"))
    .withColumn("unit_price_rank", F.dense_rank().over(w))
    .orderBy("unit_price_bucket", "unit_price_rank")
)
