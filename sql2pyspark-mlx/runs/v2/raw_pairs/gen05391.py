from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["REFUND", "DEBIT", "FEE"]))
    .select("id", "txn_type", "txn_time", F.row_number().over(w).alias("txn_time_row"))
)
