from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["REFUND", "CREDIT"]))
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 2)
    .orderBy("txn_type")
)
