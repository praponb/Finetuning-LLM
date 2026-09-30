from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .filter(F.col("account_id").isin([5, 6, 26]))
    .select("id", "merchant", "amount", "txn_type", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") == 1)
    .select("id", "merchant", "amount", "txn_type")
    .orderBy("txn_type", "merchant")
)
