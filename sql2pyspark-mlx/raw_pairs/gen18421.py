from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time")

result = (
    df_transactions
    .select("id", "txn_type", "txn_time", "amount", F.rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 2)
    .select("id", "txn_type", "txn_time", "amount")
)
