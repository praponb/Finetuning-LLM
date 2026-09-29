from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_time", "id")

result = (
    df_transactions
    .select(
        "id",
        "txn_time",
        "txn_type",
        F.first("txn_type").over(w).alias("first_txn_type"),
    )
)
