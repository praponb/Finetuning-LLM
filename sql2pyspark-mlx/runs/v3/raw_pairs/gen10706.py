from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("plan")

result = (
    df_subscriptions
    .filter(F.col("end_date") >= "2024-08-01")
    .groupBy("plan")
    .agg(
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_monthly_fee", F.sum("total_monthly_fee").over(w))
    .orderBy("total_monthly_fee", "cumulative_monthly_fee", "plan")
    .limit(10)
)
