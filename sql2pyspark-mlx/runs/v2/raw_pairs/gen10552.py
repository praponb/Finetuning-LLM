from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id", "merchant").orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "txn_time",
        "amount",
        "merchant",
        F.sum("amount").over(w).alias("running_amount"),
        F.row_number().over(w2).alias("rn"),
    )
    .orderBy("id", "account_id")
)
