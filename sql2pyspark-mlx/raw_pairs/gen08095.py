from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_transactions
    .filter(F.col("account_id").isin([2, 10, 23, 36]))
    .select(
        "id",
        "merchant",
        "txn_time",
        "amount",
        F.round(F.avg("amount").over(w), 2).alias("amount_moving_avg_3"),
    )
)
