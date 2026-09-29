from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "txn_time",
        "is_flagged",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") == 1)
    .select("id", "account_id", "txn_time", "is_flagged")
    .orderBy("account_id")
)
