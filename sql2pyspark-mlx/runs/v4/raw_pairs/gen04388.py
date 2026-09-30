from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("plan_group")

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") < 160)
    .groupBy(
        F.when(F.col("plan").isin(["premium", "basic"]), "core").otherwise("other").alias("plan_group"),
    )
    .agg(
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_monthly_fee", F.sum("total_monthly_fee").over(w))
    .orderBy(F.col("cnt").desc())
)
