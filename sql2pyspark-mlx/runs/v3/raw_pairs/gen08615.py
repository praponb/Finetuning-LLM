from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_monthly_fee").desc())

result = (
    df_subscriptions
    .filter(F.col("start_date") > "2025-09-01")
    .groupBy(F.date_format("start_date", "yyyy-MM").alias("start_month"))
    .agg(F.sum("monthly_fee").alias("total_monthly_fee"))
    .withColumn("monthly_fee_rank", F.dense_rank().over(w))
    .orderBy("monthly_fee_rank")
)
