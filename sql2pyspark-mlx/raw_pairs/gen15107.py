from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id")
w2 = Window.partitionBy("txn_type")

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "amount",
        "txn_type",
        F.round((F.col("amount") / F.sum("amount").over(w)), 3).alias("amount_share"),
        F.min("amount").over(w2).alias("txn_type_min_amount"),
    )
)
