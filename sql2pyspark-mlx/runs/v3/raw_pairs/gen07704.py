from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time")

result = (
    df_transactions
    .filter(F.col("merchant").isin(["7-Eleven", "Lazada", "Amazon"]))
    .select("id", "merchant", "txn_time", F.rank().over(w).alias("rnk"))
)
