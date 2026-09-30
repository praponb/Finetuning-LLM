from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("start_month")

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(170, 200))
    .groupBy(F.date_format("start_date", "yyyy-MM").alias("start_month"))
    .agg(
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_monthly_fee", F.sum("total_monthly_fee").over(w))
    .orderBy("start_month")
)
