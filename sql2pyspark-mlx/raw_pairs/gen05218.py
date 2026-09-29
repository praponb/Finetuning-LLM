from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(-5, Window.currentRow)

result = (
    df_accounts
    .withColumn("running_sum_balance", F.sum("balance").over(w_recent))
    .withColumn("running_avg_balance", F.avg("balance").over(w_recent))
    .withColumn("running_min_balance", F.min("balance").over(w_recent))
    .withColumn("balance_avg_5", F.avg("balance").over(w2))
)
