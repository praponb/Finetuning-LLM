from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_monthly_fee").desc())

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("plan").isin(["enterprise", "premium"]), "tier_1").otherwise("other").alias("plan_group"),
    )
    .agg(
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.count("*").alias("cnt"),
    )
    .withColumn("rnk", F.rank().over(w))
)
