from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") >= 190)
    .groupBy(
        F.when(F.col("plan").isin(["basic", "standard"]), "priority").otherwise("other").alias("plan_group"),
    )
    .agg(F.sum("monthly_fee").alias("total_monthly_fee"))
    .withColumn("pct_of_total", F.round(((F.col("total_monthly_fee") * 100) / F.sum("total_monthly_fee").over(w)), 2))
    .orderBy(F.col("total_monthly_fee").desc())
)
