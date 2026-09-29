from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").desc())

result = (
    df_transactions
    .filter(F.to_date("txn_time") < "2024-08-01")
    .select(
        "id",
        "txn_type",
        "amount",
        "merchant",
        F.dense_rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .select("id", "txn_type", "amount", "merchant")
    .orderBy(F.col("merchant").desc())
)
