from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.orderBy("amount", "id")

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "txn_time",
        "amount",
        F.row_number().over(w).alias("rn"),
        F.row_number().over(w2).alias("amount_row"),
    )
)
