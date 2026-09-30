from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(F.col("account_id").isin([4, 5, 25, 33]))
    .select(
        "id",
        "txn_time",
        "amount",
        F.lead("amount", 2, 0).over(w).alias("next_amount"),
    )
)
