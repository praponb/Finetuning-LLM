from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("YEAR", "txn_time"), "account_id").orderBy(F.col("amount").desc())
w2 = Window.partitionBy(F.date_trunc("WEEK", "txn_time")).orderBy("txn_time", "id")

result = (
    df_transactions
    .select(
        "id",
        "txn_time",
        "account_id",
        "amount",
        F.percent_rank().over(w).alias("amount_pct_rank"),
        F.lead("amount", 1).over(w2).alias("next_amount"),
    )
)
