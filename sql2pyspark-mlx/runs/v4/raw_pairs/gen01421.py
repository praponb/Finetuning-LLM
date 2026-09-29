from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc_nulls_last())
w2 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_transactions
    .select(
        "*",
        F.rank().over(w).alias("amount_rank"),
        F.sum("amount").over(w2).alias("cumulative_amount"),
    )
)

result = (
    with_windows
    .select("id", "account_id", "txn_type", "merchant", "cumulative_amount")
    .orderBy(F.col("merchant").desc())
)
