from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("account_id")

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "txn_time",
        "amount",
        F.round(F.avg("amount").over(w), 2).alias("amount_moving_avg_7"),
        F.min("amount").over(w2).alias("account_id_min_amount"),
    )
)
