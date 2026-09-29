from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id")
w2 = Window.partitionBy("account_id").orderBy("txn_time")

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        F.min("amount").over(w).alias("account_id_min_amount"),
        F.rank().over(w2).alias("rnk"),
    )
)
