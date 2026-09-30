from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("amount", "id")

result = (
    df_transactions
    .filter(F.col("txn_type") == "FEE")
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 2)
    .select("id", "account_id", "amount", "txn_time")
    .orderBy(F.col("account_id").desc())
)
