from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_transactions
    .select(
        "id",
        "merchant",
        "txn_time",
        "amount",
        "account_id",
        F.lead("amount", 1, 0).over(w).alias("next_amount"),
        F.sum("amount").over(w2).alias("running_amount"),
    )
)
