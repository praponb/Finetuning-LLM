from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "amount",
        F.sum("amount").over(w).alias("running_amount"),
    )
)
