from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(F.col("txn_time") >= "2025-06-01")
    .select(
        "id",
        "account_id",
        "txn_time",
        "amount",
        F.sum("amount").over(w).alias("running_amount"),
    )
    .orderBy("account_id")
)
