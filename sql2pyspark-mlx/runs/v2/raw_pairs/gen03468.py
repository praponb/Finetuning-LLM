from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "account_id").orderBy("txn_time", "id")

result = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "account_id",
        "txn_time",
        F.row_number().over(w).alias("txn_time_row"),
    )
)
