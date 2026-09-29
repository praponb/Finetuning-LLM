from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id")

result = (
    df_transactions
    .select(
        "id",
        "merchant",
        "txn_time",
        "txn_type",
        "amount",
        F.row_number().over(w).alias("row_num"),
        F.lead("amount", 1, 0).over(w2).alias("next_amount"),
    )
    .orderBy(F.col("txn_type").desc())
)
