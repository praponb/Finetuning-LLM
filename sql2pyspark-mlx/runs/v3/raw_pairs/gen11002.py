from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").desc())

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["FEE", "CREDIT"]))
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") <= 3)
    .select("id", "txn_type", "amount", "txn_time")
    .orderBy("txn_type")
)
