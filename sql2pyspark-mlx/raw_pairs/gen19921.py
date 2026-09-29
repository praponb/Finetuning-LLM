from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("txn_time").desc())

result = (
    df_transactions
    .select("id", "txn_type", "txn_time", "amount", F.dense_rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .orderBy(F.col("rn").desc(), F.col("amount").desc())
)
