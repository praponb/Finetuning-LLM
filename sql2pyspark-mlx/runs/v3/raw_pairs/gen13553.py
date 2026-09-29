from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(F.col("account_id").isin([9, 14, 30]))
    .select(
        "id",
        "account_id",
        "txn_time",
        "amount",
        F.lag("amount", 1).over(w).alias("prev_amount"),
    )
)
