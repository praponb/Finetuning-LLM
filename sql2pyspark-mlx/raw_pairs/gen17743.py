from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(F.col("account_id").isin([4, 17]))
    .select(
        "id",
        "merchant",
        "txn_time",
        "amount",
        "account_id",
        F.sum("amount").over(w).alias("running_amount"),
        F.lag("amount", 1).over(w2).alias("prev_amount"),
    )
    .orderBy(F.col("amount").desc())
)
