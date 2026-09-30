from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("txn_time").desc())
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id")

result = (
    df_transactions
    .select(
        "id",
        "merchant",
        "txn_time",
        "account_id",
        "amount",
        F.percent_rank().over(w).alias("txn_time_pct_rank"),
        F.lead("amount", 1).over(w2).alias("next_amount"),
    )
    .orderBy(F.col("account_id").desc(), "txn_time_pct_rank")
)
