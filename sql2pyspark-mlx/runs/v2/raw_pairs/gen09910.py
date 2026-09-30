from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount", "id")

result = (
    df_transactions
    .filter(F.col("account_id").isin([7, 15, 28, 40]))
    .select(
        "id",
        "amount",
        "txn_type",
        F.first("txn_type").over(w).alias("first_txn_type"),
    )
)
