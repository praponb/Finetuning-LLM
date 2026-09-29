from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("txn_time").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "txn_time",
        "amount",
        F.last("amount").over(w).alias("last_amount"),
    )
)
