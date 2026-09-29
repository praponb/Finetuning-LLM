from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type")

result = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "amount",
        F.sum("amount").over(w).alias("txn_type_sum_amount"),
    )
)
