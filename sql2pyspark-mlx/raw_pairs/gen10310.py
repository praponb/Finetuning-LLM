from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "account_id")
w2 = Window.partitionBy("merchant").orderBy("amount", "id")
w_running = Window.partitionBy(F.to_date("txn_time"))
w4 = Window.partitionBy("account_id", "txn_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_transactions
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w)), 4))
    .withColumn("prev_amount", F.lag("amount", 1).over(w2))
    .withColumn("txn_time_to_date_count_amount", F.count("amount").over(w_running))
    .withColumn("running_amount", F.sum("amount").over(w4))
)
