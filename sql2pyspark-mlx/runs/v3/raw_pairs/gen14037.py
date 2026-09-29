from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_year")

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy(F.year("txn_time").alias("txn_year"))
    .agg(F.sum("amount").alias("total_amount"))
    .withColumn("cumulative_amount", F.sum("total_amount").over(w))
)
