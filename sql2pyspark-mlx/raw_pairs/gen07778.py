from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant")

result = (
    df_transactions
    .filter(F.col("txn_time") >= "2025-01-28")
    .select(
        "id",
        "account_id",
        "txn_time",
        "amount",
        "merchant",
        F.sum("amount").over(w).alias("running_amount"),
        F.count("*").over(w2).alias("merchant_count"),
    )
)
