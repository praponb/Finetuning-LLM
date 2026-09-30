from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_amount").desc())

result = (
    df_orders
    .groupBy(
        F.when(F.col("discount") >= 0.42, "high").when(F.col("discount") >= 0.13, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(F.sum("amount").alias("total_amount"))
    .withColumn("rnk", F.dense_rank().over(w))
)
