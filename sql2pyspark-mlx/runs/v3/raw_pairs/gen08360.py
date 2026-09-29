from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .filter(F.col("account_id").isin([5, 26]))
    .select(
        "id",
        "merchant",
        "amount",
        "txn_time",
        F.first("txn_time").over(w).alias("first_txn_time"),
    )
    .orderBy(F.col("id").desc())
)
