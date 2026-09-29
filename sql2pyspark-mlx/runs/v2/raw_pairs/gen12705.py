from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select(
        "id",
        "account_id",
        "txn_time",
        "merchant",
        F.first("merchant").over(w).alias("first_merchant"),
    )
)
