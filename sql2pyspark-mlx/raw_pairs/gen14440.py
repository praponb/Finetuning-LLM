from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("txn_time").desc(), "id")

result = (
    df_transactions
    .filter(F.col("txn_time") < "2025-06-01")
    .select(
        "id",
        "account_id",
        "txn_time",
        "merchant",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") == 1)
    .select("id", "account_id", "txn_time", "merchant")
    .orderBy(F.col("account_id").desc())
)
