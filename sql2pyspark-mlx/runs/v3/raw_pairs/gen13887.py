from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("monthly_fee_bucket")

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 200, "high").when(F.col("monthly_fee") >= 100, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(F.sum("monthly_fee").alias("total_monthly_fee"))
    .withColumn("cumulative_monthly_fee", F.sum("total_monthly_fee").over(w))
)
