from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("shipping_fee_bucket")

result = (
    df_orders
    .groupBy(
        F.when(F.col("shipping_fee") >= 31, "high").when(F.col("shipping_fee") >= 12, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(F.sum("discount").alias("total_discount"))
    .withColumn("cumulative_discount", F.sum("total_discount").over(w))
    .orderBy(
        "total_discount",
        F.col("cumulative_discount").desc(),
        "shipping_fee_bucket",
    )
    .limit(1)
)
