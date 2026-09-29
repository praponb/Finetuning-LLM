from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_amount").desc())

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1440, "high").when(F.col("amount") >= 310, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.sum("amount").alias("total_amount"))
    .withColumn("rnk", F.rank().over(w))
)
