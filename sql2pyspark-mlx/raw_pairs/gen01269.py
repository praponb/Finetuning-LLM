from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("txn_type", "merchant").orderBy("amount", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .select(
        "id",
        "amount",
        "txn_time",
        "is_flagged",
        F.sum("amount").over(w).alias("cumulative_amount"),
        F.avg("amount").over(w2).alias("amount_avg_5"),
        F.sum("amount").over(w3).alias("cumulative_amount_2"),
        F.ntile(5).over(w4).alias("amount_bucket"),
    )
)
