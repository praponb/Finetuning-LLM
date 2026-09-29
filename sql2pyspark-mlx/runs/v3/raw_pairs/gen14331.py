from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_monthly_fee").desc())

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 239, "high").when(F.col("monthly_fee") >= 50, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.count("*").alias("cnt"),
    )
    .withColumn("rnk", F.rank().over(w))
    .orderBy(F.col("total_monthly_fee").desc(), "monthly_fee_bucket")
    .limit(20)
)
