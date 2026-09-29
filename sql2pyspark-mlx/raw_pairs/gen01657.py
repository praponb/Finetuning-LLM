from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("txn_time").desc())

result = (
    df_transactions
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "account_id", "txn_time", "merchant")
)
