from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("amount")

result = (
    df_transactions
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") <= 1)
    .select("id", "account_id", "amount")
)
