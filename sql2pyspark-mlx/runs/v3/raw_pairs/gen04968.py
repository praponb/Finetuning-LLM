from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status")

result = (
    df_subscriptions
    .filter(F.month("start_date").isin([1, 10, 11]))
    .groupBy("status")
    .agg(
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_monthly_fee", F.sum("total_monthly_fee").over(w))
    .orderBy(F.col("cumulative_monthly_fee").desc())
)
