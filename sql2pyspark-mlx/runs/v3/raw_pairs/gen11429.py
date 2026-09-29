from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_transactions
    .filter(F.col("txn_time") < "2024-09-09")
    .select(
        "id",
        "account_id",
        "txn_time",
        "amount",
        F.sum("amount").over(w).alias("running_amount"),
    )
)
