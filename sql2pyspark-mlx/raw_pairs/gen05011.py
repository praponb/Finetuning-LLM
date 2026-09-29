from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id")

result = (
    df_transactions
    .select(
        "id",
        "amount",
        "txn_type",
        "txn_time",
        F.first("txn_type").over(w).alias("first_txn_type"),
        (F.col("amount") - F.lag("amount", 1, 0).over(w2)).alias("amount_change"),
    )
    .orderBy(F.col("txn_type").desc())
)
