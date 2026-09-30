from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_time", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_transactions
    .filter(F.col("amount") > 1000)
    .select(
        "id",
        "txn_time",
        "amount",
        "merchant",
        F.round(F.avg("amount").over(w), 2).alias("amount_moving_avg_3"),
        F.sum("amount").over(w2).alias("running_amount"),
    )
    .orderBy("txn_time")
)
