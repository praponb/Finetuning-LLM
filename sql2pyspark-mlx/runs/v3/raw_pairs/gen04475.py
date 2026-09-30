from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_time", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select(
        "id",
        "txn_time",
        "amount",
        "account_id",
        F.round(F.avg("amount").over(w), 2).alias("amount_moving_avg_4"),
        F.lead("amount", 1).over(w2).alias("next_amount"),
    )
    .orderBy("account_id")
)
