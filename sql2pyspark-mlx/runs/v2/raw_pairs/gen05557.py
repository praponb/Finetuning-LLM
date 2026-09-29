from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status")

result = (
    df_subscriptions
    .groupBy("status")
    .agg(F.sum("monthly_fee").alias("total_monthly_fee"))
    .withColumn("cumulative_monthly_fee", F.sum("total_monthly_fee").over(w))
    .orderBy("total_monthly_fee")
)
