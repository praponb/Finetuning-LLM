from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("start_quarter")

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") > 110)
    .groupBy(F.quarter("start_date").alias("start_quarter"))
    .agg(
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_monthly_fee", F.sum("total_monthly_fee").over(w))
    .orderBy(F.col("cnt").desc(), "start_quarter")
    .limit(5)
)
