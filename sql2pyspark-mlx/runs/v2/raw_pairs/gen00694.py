from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_transactions
    .filter(F.col("merchant") == "Amazon")
    .select(
        "id",
        "merchant",
        "account_id",
        "txn_time",
        "amount",
        F.last("amount").over(w).alias("last_amount"),
    )
)
