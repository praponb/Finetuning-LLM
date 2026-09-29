from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type")

result = (
    df_transactions
    .select(
        "id",
        "txn_time",
        "amount",
        "txn_type",
        F.sum("amount").over(w).alias("running_amount"),
        F.count("*").over(w2).alias("txn_type_count"),
    )
)
