from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("amount", "id")

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["DEBIT", "REFUND"]))
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 2)
    .select("id", "merchant", "amount")
)
