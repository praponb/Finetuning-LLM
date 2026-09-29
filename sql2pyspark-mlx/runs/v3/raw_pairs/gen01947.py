from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time")

result = (
    df_transactions
    .select(
        "id",
        "merchant",
        "txn_time",
        F.dense_rank().over(w).alias("txn_time_dense_rank"),
    )
)
