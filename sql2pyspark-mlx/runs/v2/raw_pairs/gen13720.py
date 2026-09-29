from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("amount")

result = (
    df_transactions
    .filter(F.col("amount").between(1100, 3300))
    .select(
        "id",
        "txn_type",
        "txn_time",
        "account_id",
        "amount",
        F.first("account_id").over(w).alias("first_account_id"),
        F.percent_rank().over(w2).alias("amount_pct_rank"),
    )
    .orderBy(F.col("account_id").desc())
)
