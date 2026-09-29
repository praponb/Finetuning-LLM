from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_amount").desc())

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Amazon", "Lazada", "7-Eleven"]))
    .groupBy(
        F.when(F.col("amount") >= 3700, "high").when(F.col("amount") >= 1500, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.sum("amount").alias("total_amount"))
    .withColumn("rnk", F.dense_rank().over(w))
    .orderBy(F.col("rnk").desc(), F.col("total_amount").desc())
)
