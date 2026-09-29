from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_amount").desc())

result = (
    df_transactions
    .filter(F.col("amount") > 3800)
    .groupBy("merchant")
    .agg(F.sum("amount").alias("total_amount"))
    .withColumn("amount_rank", F.rank().over(w))
    .orderBy("merchant", "amount_rank")
)
