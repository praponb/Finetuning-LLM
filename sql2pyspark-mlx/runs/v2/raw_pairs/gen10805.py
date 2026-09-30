from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(F.col("amount") <= 3300)
    .select(
        "id",
        "account_id",
        "txn_time",
        "amount",
        F.sum("amount").over(w).alias("running_amount"),
    )
)
