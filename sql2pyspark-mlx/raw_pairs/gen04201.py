from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time")

result = (
    df_transactions
    .filter(F.col("txn_type") != "DEBIT")
    .select("id", "account_id", "txn_time", F.rank().over(w).alias("txn_time_rank"))
)
