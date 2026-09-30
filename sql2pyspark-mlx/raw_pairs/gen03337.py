from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "txn_time",
        "amount",
        (F.col("amount") - F.lag("amount", 2).over(w)).alias("amount_change"),
    )
    .orderBy("amount_change")
)
