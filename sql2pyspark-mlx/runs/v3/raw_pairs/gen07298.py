from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(F.col("account_id") == 4)
    .select("id", "txn_time", "amount", F.lag("amount", 2).over(w).alias("prev_amount"))
    .orderBy(F.col("amount").desc())
)
