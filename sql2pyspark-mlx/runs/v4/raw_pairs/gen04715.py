from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(F.col("amount") <= 3400)
    .select("id", "txn_type", "txn_time", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 5)
    .select("id", "txn_type", "txn_time")
    .orderBy("txn_type")
)
