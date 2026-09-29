from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_amount").desc())

result = (
    df_transactions
    .groupBy("txn_type")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("cnt"),
    )
    .withColumn("rnk", F.rank().over(w))
    .orderBy("txn_type", F.col("total_amount").desc())
)
