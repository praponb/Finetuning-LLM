from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "account_id")

result = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "account_id",
        "amount",
        F.max("amount").over(w).alias("txn_type_max_amount"),
    )
    .orderBy(F.col("txn_type_max_amount").desc())
)
