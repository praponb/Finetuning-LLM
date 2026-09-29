from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_transactions
    .filter(F.col("txn_type") == "CREDIT")
    .select(
        "id",
        "txn_type",
        "txn_time",
        "amount",
        F.round(F.avg("amount").over(w), 2).alias("amount_moving_avg_4"),
    )
)
