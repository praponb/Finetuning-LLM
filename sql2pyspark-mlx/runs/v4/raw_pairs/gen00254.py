from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")

result = (
    df_transactions
    .select("id", "merchant", "txn_time", "amount", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 5)
    .select("id", "merchant", "txn_time", "amount")
)
