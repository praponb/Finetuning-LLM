from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("txn_time").desc(), "id")

result = (
    df_transactions
    .filter(F.col("merchant") == "7-Eleven")
    .select(
        "id",
        "merchant",
        "txn_time",
        "account_id",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 3)
    .select("id", "merchant", "txn_time", "account_id")
    .orderBy(F.col("txn_time").desc())
)
