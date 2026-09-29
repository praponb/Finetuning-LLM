from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["REFUND", "FEE"]))
    .select(
        "id",
        "merchant",
        "amount",
        "txn_time",
        F.last("txn_time").over(w).alias("last_txn_time"),
    )
)
