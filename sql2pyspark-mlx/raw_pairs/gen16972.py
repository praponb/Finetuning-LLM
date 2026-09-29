from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("amount")
w2 = Window.partitionBy("account_id").orderBy(F.col("txn_time").desc())

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        F.percent_rank().over(w).alias("amount_pct_rank"),
        F.percent_rank().over(w2).alias("txn_time_pct_rank"),
    )
)
