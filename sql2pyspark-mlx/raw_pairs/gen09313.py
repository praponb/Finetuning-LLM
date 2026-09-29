from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").desc())
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.year("txn_time")).orderBy("txn_time", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        F.rank().over(w).alias("amount_rank"),
        F.first("amount", ignorenulls=True).over(w2).alias("first_known_amount"),
        F.avg("amount").over(w3).alias("amount_avg_3"),
    )
)
