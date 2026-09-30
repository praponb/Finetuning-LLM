from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_unit_price").desc())

result = (
    df_order_items
    .filter(F.col("order_id") == 13)
    .groupBy(
        F.when(F.col("unit_price") >= 310, "high").when(F.col("unit_price") >= 110, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.sum("unit_price").alias("total_unit_price"))
    .withColumn("rnk", F.dense_rank().over(w))
)
