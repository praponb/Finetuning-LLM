from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_transactions
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        F.row_number().over(w).alias("row_seq"),
        F.sum("amount").over(w2).alias("running_amount"),
    )
)

result = metrics.select("id", "account_id", "txn_type", "amount", "txn_time", "row_seq")
