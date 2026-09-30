from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_time")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .select(
        "id",
        "txn_time",
        "account_id",
        F.rank().over(w).alias("txn_time_rank"),
        F.row_number().over(w2).alias("row_num"),
    )
    .orderBy("id", F.col("row_num").desc())
)
