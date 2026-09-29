from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")

result = (
    df_transactions
    .select("id", "account_id", "txn_time", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .select("id", "account_id", "txn_time")
)
