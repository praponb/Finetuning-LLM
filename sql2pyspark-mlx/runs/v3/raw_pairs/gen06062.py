from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount_bucket")

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 4000, "high").when(F.col("amount") >= 1200, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.sum("amount").alias("total_amount"))
    .withColumn("cumulative_amount", F.sum("total_amount").over(w))
    .orderBy("amount_bucket")
)
