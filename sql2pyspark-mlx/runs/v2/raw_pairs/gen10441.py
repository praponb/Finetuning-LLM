from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "merchant").orderBy("txn_time", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy(F.col("txn_time").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_transactions
    .filter(F.col("txn_time") < "2025-11-01")
    .select(
        "id",
        "account_id",
        "merchant",
        "txn_time",
        "amount",
        "txn_type",
        F.round(F.avg("amount").over(w), 2).alias("amount_moving_avg_7"),
        F.last("txn_type").over(w2).alias("last_txn_type"),
    )
)
