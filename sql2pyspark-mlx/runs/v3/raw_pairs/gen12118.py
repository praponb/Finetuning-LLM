from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("merchant")

result = (
    df_transactions
    .filter(F.col("txn_type") != "DEBIT")
    .groupBy("merchant")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_amount", F.sum("total_amount").over(w))
    .orderBy(F.col("cumulative_amount").desc())
)
