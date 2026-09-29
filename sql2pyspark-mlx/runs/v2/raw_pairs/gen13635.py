from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type")

result = (
    df_transactions
    .withColumn("avg_amount_by_txn_type", F.avg("amount").over(w))
    .filter(F.col("amount") < F.col("avg_amount_by_txn_type"))
    .select("id", "txn_type", "amount")
    .orderBy("id")
)
