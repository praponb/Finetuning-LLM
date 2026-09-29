from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time")

result = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "txn_time",
        "account_id",
        F.dense_rank().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 2)
    .select("id", "txn_type", "txn_time", "account_id")
    .orderBy(F.col("id").desc())
)
