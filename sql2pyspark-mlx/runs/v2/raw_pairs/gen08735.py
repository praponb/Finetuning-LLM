from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("balance").desc())

result = (
    df_accounts
    .filter(F.col("balance") <= 29000)
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "account_type", "balance", "status")
)
