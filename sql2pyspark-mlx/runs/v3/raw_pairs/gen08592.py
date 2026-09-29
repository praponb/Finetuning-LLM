from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time")
w2 = Window.partitionBy("merchant").orderBy("amount", "id")

result = (
    df_transactions
    .filter(F.col("account_id") == 10)
    .select(
        "id",
        "merchant",
        "txn_time",
        "amount",
        F.rank().over(w).alias("rnk"),
        F.row_number().over(w2).alias("rn"),
    )
    .orderBy("rn")
)
