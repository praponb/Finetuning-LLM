from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("txn_time").desc())

result = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select("id", "merchant", "txn_time", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 2)
    .select("id", "merchant", "txn_time")
    .orderBy("id")
)
