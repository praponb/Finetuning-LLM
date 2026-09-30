from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type")
w2 = Window.orderBy("txn_time", "id")

result = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        F.sum("amount").over(w).alias("txn_type_sum_amount"),
        F.row_number().over(w2).alias("rn"),
    )
)
