from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_subscriptions
    .filter(F.col("start_date") <= "2024-03-01")
    .groupBy(
        F.when(F.col("monthly_fee") >= 267, "high").when(F.col("monthly_fee") >= 60, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(F.sum("monthly_fee").alias("total_monthly_fee"))
    .withColumn("pct_of_total", F.round(((F.col("total_monthly_fee") * 100) / F.sum("total_monthly_fee").over(w)), 2))
    .orderBy("monthly_fee_bucket", F.col("total_monthly_fee").desc())
)
