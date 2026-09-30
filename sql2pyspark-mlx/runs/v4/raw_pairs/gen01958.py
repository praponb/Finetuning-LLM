from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(F.col("account_id").isin([6, 35]))
    .select("id", "merchant", "txn_time", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
    .select("id", "merchant", "txn_time")
)
