from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("txn_time"), "merchant").orderBy("txn_time", "id")

result = (
    df_transactions
    .select(
        "id",
        "txn_time",
        "merchant",
        "amount",
        F.lag("amount", 2).over(w).alias("prev_amount"),
    )
)
