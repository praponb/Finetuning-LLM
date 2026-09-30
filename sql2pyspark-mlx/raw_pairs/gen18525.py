from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(F.hour("txn_time").between(10, 13))
    .select(
        "id",
        "txn_time",
        "amount",
        "merchant",
        (F.col("amount") - F.lag("amount", 1).over(w)).alias("amount_change"),
        (F.col("amount") - F.lag("amount", 1, 0).over(w2)).alias("amount_change_2"),
    )
    .orderBy(F.col("txn_time").desc())
)
