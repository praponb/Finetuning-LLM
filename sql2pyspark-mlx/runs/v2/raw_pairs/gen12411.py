from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("amount", "id")

result = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "amount",
        "account_id",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .select("id", "txn_type", "amount", "account_id")
    .orderBy("amount")
)
