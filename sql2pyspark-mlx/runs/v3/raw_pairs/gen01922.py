from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_amount").desc())

result = (
    df_transactions
    .groupBy("account_id")
    .agg(F.sum("amount").alias("total_amount"))
    .withColumn("amount_rank", F.rank().over(w))
    .orderBy("total_amount", "account_id")
    .limit(10)
)
