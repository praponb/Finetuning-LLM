from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("txn_time").desc())

result = (
    df_transactions
    .filter(~F.col("merchant").isin(["Amazon", "Grab"]))
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") == 1)
    .select("id", "account_id", "txn_time", "txn_type")
    .orderBy("txn_time")
)
