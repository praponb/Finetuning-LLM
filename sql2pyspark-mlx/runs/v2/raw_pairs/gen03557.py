from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("merchant")

result = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "txn_time",
        "amount",
        "merchant",
        F.round(F.avg("amount").over(w), 2).alias("amount_moving_avg_4"),
        F.sum("amount").over(w2).alias("merchant_sum_amount"),
    )
    .orderBy(F.col("amount_moving_avg_4").desc())
)
