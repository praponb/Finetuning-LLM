from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Amazon", "7-Eleven", "Grab"]))
    .select(
        "id",
        "txn_type",
        "txn_time",
        "amount",
        F.lag("amount", 1).over(w).alias("prev_amount"),
    )
)
