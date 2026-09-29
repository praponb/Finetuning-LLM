from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("txn_time").desc(), "id")

result = (
    df_transactions
    .filter(F.col("account_id").isin([5, 11]))
    .select("id", "account_id", "txn_time", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") == 1)
    .select("id", "account_id", "txn_time")
)
