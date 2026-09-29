from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy(F.col("txn_time").desc())

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "txn_time",
        "amount",
        "merchant",
        F.lag("amount", 2).over(w).alias("prev_amount"),
        F.rank().over(w2).alias("txn_time_rank"),
    )
    .orderBy(F.col("txn_time_rank").desc())
)
