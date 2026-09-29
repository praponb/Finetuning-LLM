from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(F.col("amount") >= 1500)
    .select("id", "txn_time", "amount", F.sum("amount").over(w).alias("running_amount"))
)
