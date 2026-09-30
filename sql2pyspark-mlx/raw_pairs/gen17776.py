from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id")

result = (
    df_transactions
    .select(
        "id",
        "txn_time",
        "amount",
        "txn_type",
        F.lead("amount", 2).over(w).alias("next_amount"),
        F.lag("amount", 2, 0).over(w2).alias("prev_amount"),
    )
)
