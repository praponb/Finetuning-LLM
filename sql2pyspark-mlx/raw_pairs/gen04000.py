from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("txn_time"), "account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_transactions
    .select(
        "id",
        "txn_time",
        "account_id",
        "amount",
        F.lag("amount", 2).over(w).alias("prev_amount"),
        F.round(F.avg("amount").over(w2), 2).alias("amount_moving_avg_7"),
    )
)
