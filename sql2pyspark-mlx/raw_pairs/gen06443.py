from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy(F.col("amount").desc())

result = (
    df_transactions
    .select(
        "id",
        "txn_time",
        "amount",
        "txn_type",
        F.lead("amount", 1).over(w).alias("next_amount"),
        F.dense_rank().over(w2).alias("amount_dense_rank"),
    )
)
