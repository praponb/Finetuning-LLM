from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id")
w_recent = Window.partitionBy(F.to_date("txn_time")).orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.min("amount").over(w).alias("account_id_min_amount"),
        F.last("merchant", ignorenulls=True).over(w_recent).alias("last_known_merchant"),
    )
)

result = (
    metrics
    .select("id", "amount", "txn_time", "merchant", "account_id_min_amount")
    .orderBy(F.col("merchant").desc(), "txn_time")
)
