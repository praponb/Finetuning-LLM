from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_monthly_fee").desc())

result = (
    df_subscriptions
    .groupBy("status")
    .agg(F.sum("monthly_fee").alias("total_monthly_fee"))
    .withColumn("monthly_fee_rank", F.dense_rank().over(w))
)
