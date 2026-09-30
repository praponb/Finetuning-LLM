from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("txn_time").desc(), "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time")

result = (
    df_transactions
    .filter(F.col("merchant") == "Lazada")
    .select(
        "id",
        "account_id",
        "txn_time",
        "txn_type",
        F.row_number().over(w).alias("row_num"),
        F.percent_rank().over(w2).alias("txn_time_pct_rank"),
    )
)
