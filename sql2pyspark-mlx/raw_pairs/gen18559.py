from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time")

deduped = (
    df_transactions
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "txn_time")
)

result = (
    deduped
    .select("id", "txn_time")
    .orderBy("txn_time", "id")
)
