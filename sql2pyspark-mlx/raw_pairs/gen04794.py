from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("merchant", "txn_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Shell", "Lazada", "7-Eleven"]))
    .withColumn("running_min_amount", F.min("amount").over(w_part))
    .withColumn("running_avg_amount", F.avg("amount").over(w_part))
    .withColumn("running_sum_amount", F.sum("amount").over(w_part))
    .withColumn("running_max_amount", F.max("amount").over(w_part))
    .withColumn("first_known_amount", F.first("amount", ignorenulls=True).over(w2))
)
