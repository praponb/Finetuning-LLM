from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")

result = (
    df_transactions
    .select(
        "id",
        "merchant",
        "txn_time",
        "amount",
        F.sum("amount").over(w).alias("running_amount"),
    )
)
