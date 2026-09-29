from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy(F.month("txn_time"), "account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("merchant", "txn_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        F.last("merchant", ignorenulls=True).over(w).alias("last_known_merchant"),
        F.sum("amount").over(w_part).alias("cumulative_amount"),
        F.sum("amount").over(w_hist).alias("running_amount"),
        F.first("amount", ignorenulls=True).over(w4).alias("first_known_amount"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        "running_amount",
        "first_known_amount",
        F.when(F.col("txn_type").rlike("^(DEBIT|REFUND)$"), "GROUP_B").when(F.col("txn_type").rlike("^(CREDIT|FEE)$"), "PRIORITY").otherwise("CORE").alias("txn_type_zone"),
    )
    .orderBy(F.col("running_amount").desc_nulls_last())
)
