from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("account_id")

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy("account_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_amount", F.sum("total_amount").over(w))
    .orderBy("account_id", F.col("total_amount").desc())
)
