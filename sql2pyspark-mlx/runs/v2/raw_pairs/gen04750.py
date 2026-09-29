from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_subscriptions
    .filter(F.col("customer_id") == 17)
    .groupBy("status")
    .agg(F.sum("monthly_fee").alias("total_monthly_fee"))
    .withColumn("pct_of_total", F.round(((F.col("total_monthly_fee") * 100) / F.sum("total_monthly_fee").over(w)), 2))
    .orderBy(F.col("status").desc())
)
