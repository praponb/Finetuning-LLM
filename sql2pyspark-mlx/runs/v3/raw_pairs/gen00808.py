from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_hour")

result = (
    df_transactions
    .groupBy(F.hour("txn_time").alias("txn_hour"))
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_amount", F.sum("total_amount").over(w))
    .orderBy(F.col("total_amount").desc(), F.col("cumulative_amount").desc())
)
