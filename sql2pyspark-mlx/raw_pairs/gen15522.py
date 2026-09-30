from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w2 = Window.partitionBy("account_id")
w_running = Window.orderBy("amount", "id")

result = (
    df_transactions
    .filter(F.col("txn_time") >= "2023-07-17")
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        F.sum("amount").over(w).alias("amount_sum_90d"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 4).alias("amount_share"),
        F.ntile(5).over(w_running).alias("amount_bucket"),
    )
)
