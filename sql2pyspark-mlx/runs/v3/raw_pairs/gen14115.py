from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time")

result = (
    df_transactions
    .filter(F.col("amount") > 3500)
    .select(
        "id",
        "merchant",
        "txn_time",
        F.percent_rank().over(w).alias("txn_time_pct_rank"),
    )
    .orderBy("merchant")
)
