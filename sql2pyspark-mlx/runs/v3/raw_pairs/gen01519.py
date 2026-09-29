from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_transactions
    .filter(F.col("account_id").isin([17, 40]))
    .groupBy("is_flagged")
    .agg(F.sum("amount").alias("total_amount"))
    .withColumn("pct_of_total", F.round(((F.col("total_amount") * 100) / F.sum("total_amount").over(w)), 2))
    .orderBy("total_amount")
)
