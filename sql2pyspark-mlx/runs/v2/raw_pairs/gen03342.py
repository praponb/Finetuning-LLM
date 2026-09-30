from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("txn_time").desc())
w2 = Window.partitionBy("txn_type").orderBy(F.col("txn_time").desc())

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "txn_time",
        "txn_type",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.percent_rank().over(w2).alias("txn_time_pct_rank"),
    )
)
