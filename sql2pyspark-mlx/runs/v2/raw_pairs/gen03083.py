from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("amount", "id")

result = (
    df_transactions
    .filter(F.col("merchant") != "Lazada")
    .select(
        "id",
        "txn_type",
        "txn_time",
        "amount",
        "merchant",
        F.lag("amount", 1).over(w).alias("prev_amount"),
        F.row_number().over(w2).alias("rn"),
    )
)
