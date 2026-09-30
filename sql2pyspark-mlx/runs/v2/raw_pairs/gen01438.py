from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy(F.col("amount").desc())

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "amount",
        "merchant",
        (F.col("amount") - F.lag("amount", 1, 0).over(w)).alias("amount_change"),
        F.dense_rank().over(w2).alias("amount_dense_rank"),
    )
)
