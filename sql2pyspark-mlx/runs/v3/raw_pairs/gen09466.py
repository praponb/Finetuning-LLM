from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "account_id").orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(F.col("account_id").isin([5, 8, 26]))
    .select(
        "id",
        "merchant",
        "account_id",
        "txn_time",
        "amount",
        F.lead("amount", 2).over(w).alias("next_amount"),
    )
)
