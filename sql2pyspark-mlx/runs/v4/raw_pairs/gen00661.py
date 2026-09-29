from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(F.col("merchant") == "Grab")
    .select("id", "txn_time", "amount", F.lag("amount", 1).over(w).alias("prev_amount"))
)
