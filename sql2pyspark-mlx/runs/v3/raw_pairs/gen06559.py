from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount_bucket")

result = (
    df_payments
    .filter(F.col("paid_at") >= "2023-10-01")
    .groupBy(
        F.when(F.col("amount") >= 1330, "high").when(F.col("amount") >= 440, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_amount", F.sum("total_amount").over(w))
    .orderBy(F.col("total_amount").desc(), "cumulative_amount")
)
