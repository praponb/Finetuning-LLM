from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "txn_time",
        "amount",
        F.ntile(10).over(w).alias("quartile"),
        F.last("amount").over(w2).alias("last_amount"),
    )
    .orderBy("id", "last_amount")
)
