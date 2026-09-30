from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant")
w2 = Window.partitionBy("account_id", "txn_type").orderBy("txn_time", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_transactions
    .filter(F.to_date("txn_time") >= "2025-09-26")
    .select(
        "id",
        "merchant",
        "amount",
        "account_id",
        "txn_type",
        "txn_time",
        F.avg("amount").over(w).alias("merchant_avg_amount"),
        F.round(F.avg("amount").over(w2), 2).alias("amount_moving_avg_3"),
    )
)
