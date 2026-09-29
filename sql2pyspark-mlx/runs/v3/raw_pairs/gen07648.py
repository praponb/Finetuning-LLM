from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_transactions
    .filter(F.col("account_id").isin([22, 24, 29, 33]))
    .select(
        "id",
        "account_id",
        "txn_time",
        "amount",
        F.round(F.avg("amount").over(w), 2).alias("amount_moving_avg_7"),
    )
)
