from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "account_id").orderBy(F.col("txn_time").desc())

result = (
    df_transactions
    .select(
        "id",
        "merchant",
        "account_id",
        "txn_time",
        F.percent_rank().over(w).alias("txn_time_pct_rank"),
    )
)
