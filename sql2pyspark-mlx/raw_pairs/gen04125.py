from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy(F.month("txn_time"), "account_id").orderBy(F.col("amount").desc_nulls_first())

result = (
    df_transactions
    .filter(F.col("amount").isNull())
    .withColumn("running_sum_amount", F.sum("amount").over(w_recent))
    .withColumn("running_max_amount", F.max("amount").over(w_recent))
    .withColumn("running_avg_amount", F.avg("amount").over(w_recent))
    .withColumn("amount_cume_dist", F.cume_dist().over(w_part))
)
