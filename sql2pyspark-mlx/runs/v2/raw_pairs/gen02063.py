from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time")

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Grab", "Amazon"]))
    .select(
        "id",
        "account_id",
        "txn_time",
        F.dense_rank().over(w).alias("txn_time_dense_rank"),
    )
)
