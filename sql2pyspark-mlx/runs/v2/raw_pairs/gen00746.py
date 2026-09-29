from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_transactions
    .filter(F.col("txn_type") != "FEE")
    .select(
        "id",
        "txn_type",
        "txn_time",
        "account_id",
        F.last("account_id").over(w).alias("last_account_id"),
    )
)
