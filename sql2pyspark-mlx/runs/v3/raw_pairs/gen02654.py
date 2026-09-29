from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("txn_time").desc())

result = (
    df_transactions
    .filter(F.col("txn_type") == "REFUND")
    .select("id", "merchant", "txn_time", F.rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 3)
    .select("id", "merchant", "txn_time")
    .orderBy("merchant")
)
