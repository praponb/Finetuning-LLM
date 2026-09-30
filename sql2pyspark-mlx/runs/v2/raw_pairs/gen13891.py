from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("txn_time").desc())

result = (
    df_transactions
    .select("id", "account_id", "txn_time", "txn_type", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 5)
    .select("id", "account_id", "txn_time", "txn_type")
)
