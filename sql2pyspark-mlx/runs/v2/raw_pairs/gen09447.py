from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc())

result = (
    df_transactions
    .filter(F.to_date("txn_time") >= "2025-07-01")
    .select(
        "id",
        "merchant",
        "amount",
        F.dense_rank().over(w).alias("amount_dense_rank"),
    )
    .orderBy("merchant")
)
