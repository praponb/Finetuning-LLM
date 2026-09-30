from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_day")

result = (
    df_transactions
    .groupBy(F.to_date("txn_time").alias("txn_day"))
    .agg(F.sum("amount").alias("total_amount"))
    .withColumn("cumulative_amount", F.sum("total_amount").over(w))
    .orderBy("cumulative_amount")
)
