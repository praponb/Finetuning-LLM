from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("txn_type", "account_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "txn_time",
        F.max("amount").over(w_recent).alias("running_max_amount"),
        F.sum("amount").over(w_recent).alias("running_sum_amount"),
        F.min("amount").over(w_recent).alias("running_min_amount"),
        F.avg("amount").over(w_recent).alias("running_avg_amount"),
    )
)
