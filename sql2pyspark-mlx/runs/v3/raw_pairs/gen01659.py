from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_amount").desc())

result = (
    df_transactions
    .filter(F.col("account_id").isin([15, 18]))
    .groupBy("merchant")
    .agg(F.sum("amount").alias("total_amount"))
    .withColumn("amount_rank", F.dense_rank().over(w))
)
