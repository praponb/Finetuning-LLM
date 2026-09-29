from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_transactions
    .filter(F.col("account_id").isin([11, 16, 26]))
    .select(
        "id",
        "amount",
        "merchant",
        F.last("merchant").over(w).alias("last_merchant"),
    )
    .orderBy("merchant", F.col("amount").desc())
)
