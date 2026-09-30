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
        F.lead("amount", 1).over(w).alias("next_amount"),
    )
)
