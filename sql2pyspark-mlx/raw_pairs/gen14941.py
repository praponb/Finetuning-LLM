from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy(F.year("txn_time")).orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id").orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "txn_type",
        "is_flagged",
        F.ntile(4).over(w).alias("amount_bucket"),
        F.sum("amount").over(w2).alias("cumulative_amount"),
        F.row_number().over(w3).alias("row_seq"),
    )
)
