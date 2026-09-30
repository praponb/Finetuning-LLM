from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_amount").desc())

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 4300, "high").when(F.col("amount") >= 1700, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.sum("amount").alias("total_amount"))
    .withColumn("amount_rank", F.rank().over(w))
    .orderBy("amount_bucket")
)
