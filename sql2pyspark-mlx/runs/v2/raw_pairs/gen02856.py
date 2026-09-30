from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_amount").desc())

result = (
    df_payments
    .filter(F.col("method") == "bank_transfer")
    .groupBy("method")
    .agg(F.sum("amount").alias("total_amount"))
    .withColumn("rnk", F.rank().over(w))
)
