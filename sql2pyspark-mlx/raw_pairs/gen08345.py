from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("account_id")
w3 = Window.partitionBy("merchant", "txn_type").orderBy(F.col("txn_time").desc(), "id")

result = (
    df_transactions
    .filter(F.col("account_id") == 28)
    .select(
        "id",
        "account_id",
        "txn_time",
        "merchant",
        F.first("amount", ignorenulls=True).over(w).alias("first_known_amount"),
        F.min("amount").over(w2).alias("account_id_min_amount"),
        F.min("amount").over(w2).alias("account_id_min_amount_2"),
        F.row_number().over(w3).alias("seq_no"),
    )
)
