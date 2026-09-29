from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type")
w2 = Window.partitionBy("merchant").orderBy(F.col("txn_time").desc())

result = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "amount",
        "merchant",
        "txn_time",
        F.sum("amount").over(w).alias("txn_type_sum_amount"),
        F.rank().over(w2).alias("txn_time_rank"),
    )
)
