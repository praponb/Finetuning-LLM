from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id")

result = (
    df_accounts
    .filter(F.col("customer_id").isin([13, 24]))
    .withColumn("running_sum_balance", F.sum("balance").over(w))
    .withColumn("running_min_balance", F.min("balance").over(w))
    .withColumn("running_count_balance", F.count("balance").over(w))
    .withColumn("customer_id_min_balance", F.min("balance").over(w2))
)
