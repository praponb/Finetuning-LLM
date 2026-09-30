from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w_running = Window.partitionBy(F.month("txn_time"), "txn_type").orderBy("amount", "id")
w_part = Window.partitionBy(F.to_date("txn_time"), "txn_type").orderBy("amount", "id").rowsBetween(-6, -1)
w4 = Window.orderBy(F.col("amount").desc())
w5 = Window.orderBy("txn_time", "id").rowsBetween(-5, Window.currentRow)

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "txn_time",
        "merchant",
        "is_flagged",
        F.lead("amount", 1).over(w).alias("next_amount"),
        F.ntile(3).over(w_running).alias("amount_bucket"),
        F.avg("amount").over(w_part).alias("amount_avg_6_prior"),
        F.dense_rank().over(w4).alias("amount_dense_rank"),
        F.avg("amount").over(w5).alias("amount_avg_5"),
    )
)
