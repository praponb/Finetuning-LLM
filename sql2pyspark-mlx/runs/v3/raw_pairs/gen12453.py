from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "txn_type").orderBy("txn_time", "id")

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "amount",
        F.lag("amount", 1).over(w).alias("prev_amount"),
    )
    .orderBy(F.col("txn_time").desc())
)
