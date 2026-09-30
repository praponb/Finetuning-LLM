from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "txn_type")

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        F.max("amount").over(w).alias("account_id_max_amount"),
    )
)
