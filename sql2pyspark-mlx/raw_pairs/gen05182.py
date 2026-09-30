from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "txn_type").orderBy(F.col("amount").desc())

result = (
    df_transactions
    .select("id", "account_id", "txn_type", "amount", F.rank().over(w).alias("rnk"))
)
