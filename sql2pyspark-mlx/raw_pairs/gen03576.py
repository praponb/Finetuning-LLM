from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("amount").desc_nulls_last())

result = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "is_flagged",
        F.last("amount", ignorenulls=True).over(w).alias("last_known_amount"),
        F.dense_rank().over(w2).alias("amount_dense_rank"),
    )
)
