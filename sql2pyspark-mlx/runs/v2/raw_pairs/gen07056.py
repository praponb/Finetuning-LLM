from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("txn_time").desc())

result = (
    df_transactions
    .select("id", "txn_type", "txn_time", F.dense_rank().over(w).alias("dense_rnk"))
)
