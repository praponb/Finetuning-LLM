from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc())

result = (
    df_transactions
    .select("id", "merchant", "amount", F.dense_rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 1)
    .select("id", "merchant", "amount")
    .orderBy(F.col("id").desc())
)
