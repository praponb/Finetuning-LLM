from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type")

result = (
    df_accounts
    .withColumn("min_balance_by_account_type", F.min("balance").over(w))
    .filter(F.col("balance") == F.col("min_balance_by_account_type"))
    .select("id", "account_type", "balance")
    .orderBy("balance", F.col("account_type").desc())
)
