from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_transactions
    .select(
        "id",
        "txn_time",
        "amount",
        "merchant",
        F.sum("amount").over(w).alias("running_amount"),
        F.round(F.avg("amount").over(w2), 2).alias("amount_moving_avg_3"),
    )
)
