from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["DEBIT", "FEE", "CREDIT"]))
    .select(
        "id",
        "merchant",
        "txn_time",
        "amount",
        F.row_number().over(w).alias("txn_time_row"),
        F.round(F.avg("amount").over(w2), 2).alias("amount_moving_avg_7"),
    )
)
