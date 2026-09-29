from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        F.last("txn_time").over(w).alias("last_txn_time"),
    )
    .orderBy("txn_time")
)
