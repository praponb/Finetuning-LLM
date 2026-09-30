from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "txn_time",
        "merchant",
        "is_flagged",
        F.max("amount").over(w_recent).alias("running_max_amount"),
        F.count("amount").over(w_recent).alias("running_count_amount"),
        F.min("amount").over(w_recent).alias("running_min_amount"),
    )
)
