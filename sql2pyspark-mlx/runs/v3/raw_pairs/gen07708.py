from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["REFUND", "FEE", "CREDIT"]))
    .select(
        "id",
        "amount",
        "txn_type",
        F.last("txn_type").over(w).alias("last_txn_type"),
    )
)
