from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_time", "id")

result = (
    df_transactions
    .select(
        "id",
        "txn_time",
        "amount",
        (F.col("amount") - F.lag("amount", 1).over(w)).alias("amount_change"),
    )
    .orderBy("amount_change", F.col("amount").desc())
)
