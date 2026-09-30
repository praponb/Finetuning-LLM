from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("txn_time").desc(), "id")

result = (
    df_transactions
    .filter(F.col("merchant").isin(["7-Eleven", "Amazon", "Lazada"]))
    .select(
        "id",
        "account_id",
        "txn_time",
        F.row_number().over(w).alias("txn_time_row"),
    )
)
