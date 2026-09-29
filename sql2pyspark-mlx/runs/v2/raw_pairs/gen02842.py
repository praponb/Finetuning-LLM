from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("txn_type")

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "txn_time",
        "amount",
        "txn_type",
        F.round(F.avg("amount").over(w), 2).alias("amount_moving_avg_7"),
        F.max("amount").over(w2).alias("txn_type_max_amount"),
    )
    .orderBy("txn_time", F.col("amount_moving_avg_7").desc())
)
