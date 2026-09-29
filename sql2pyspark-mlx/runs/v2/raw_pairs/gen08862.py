from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_monthly_fee").desc())

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([1, 26, 35, 38]))
    .groupBy("customer_id")
    .agg(
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.count("*").alias("cnt"),
    )
    .withColumn("monthly_fee_rank", F.dense_rank().over(w))
)
