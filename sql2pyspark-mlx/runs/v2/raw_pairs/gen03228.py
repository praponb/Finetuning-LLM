from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(F.col("amount") > 3000)
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 1)
    .select("id", "account_id", "txn_time", "is_flagged")
    .orderBy(F.col("account_id").desc(), "txn_time")
)
