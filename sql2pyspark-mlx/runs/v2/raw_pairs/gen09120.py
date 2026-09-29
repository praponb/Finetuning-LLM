from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_orders
    .groupBy(
        F.when(F.col("amount") >= 1360, "high").when(F.col("amount") >= 580, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("cnt"),
    )
    .withColumn("pct_of_total", F.round(((F.col("total_amount") * 100) / F.sum("total_amount").over(w)), 2))
    .orderBy(F.col("cnt").desc(), "amount_bucket")
    .limit(1)
)
