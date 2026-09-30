from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("txn_time").desc(), "id")

result = (
    df_transactions
    .select(
        "id",
        "txn_time",
        "account_id",
        F.first("account_id").over(w).alias("first_account_id"),
    )
)
