from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "txn_type").orderBy("amount")

result = (
    df_transactions
    .filter(F.col("txn_time") < "2024-10-01")
    .select("id", "account_id", "txn_type", "amount", F.rank().over(w).alias("rnk"))
)
