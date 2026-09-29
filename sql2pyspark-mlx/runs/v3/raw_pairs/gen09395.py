from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time")

result = (
    df_transactions
    .filter(F.to_date("txn_time") >= "2025-09-02")
    .select("id", "txn_type", "txn_time", F.rank().over(w).alias("txn_time_rank"))
)
