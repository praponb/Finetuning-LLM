from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("amount")

result = (
    df_transactions
    .filter(F.col("account_id").isin([5, 37]))
    .select("id", "merchant", "amount", F.dense_rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 2)
    .orderBy("rn")
)
