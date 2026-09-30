from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("txn_time"), "account_id").orderBy("txn_time")
w2 = Window.partitionBy("merchant")

result = (
    df_transactions
    .filter(F.to_date("txn_time") < "2023-10-01")
    .select(
        "id",
        "txn_time",
        "account_id",
        "merchant",
        "amount",
        F.rank().over(w).alias("txn_time_rank"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 3).alias("amount_share"),
    )
)
