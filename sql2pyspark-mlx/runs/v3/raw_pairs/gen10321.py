from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_amount").desc())

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy(F.month("txn_time").alias("txn_month"))
    .agg(F.sum("amount").alias("total_amount"))
    .withColumn("rnk", F.dense_rank().over(w))
    .orderBy("rnk", F.col("total_amount").desc(), "txn_month")
    .limit(10)
)
