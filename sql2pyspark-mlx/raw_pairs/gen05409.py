from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("account_id", "merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_transactions
    .select(
        "id",
        "txn_time",
        "merchant",
        "is_flagged",
        F.min("amount").over(w).alias("last7_min_amount"),
        F.avg("amount").over(w).alias("last7_avg_amount"),
        F.max("amount").over(w).alias("last7_max_amount"),
        F.last("amount", ignorenulls=True).over(w2).alias("last_known_amount"),
    )
)
