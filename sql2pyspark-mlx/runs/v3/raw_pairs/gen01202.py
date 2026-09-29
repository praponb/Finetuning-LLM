from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 4100, "high").when(F.col("amount") >= 600, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.sum("amount").alias("total_amount"))
    .withColumn("pct_of_total", F.round(((F.col("total_amount") * 100) / F.sum("total_amount").over(w)), 2))
)
