from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "status").orderBy("balance", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("customer_id").orderBy(F.col("balance").desc())
w3 = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("balance", "id")
w5 = Window.partitionBy("account_type").orderBy(F.col("balance").desc(), "id")

result = (
    df_accounts
    .withColumn("balance_avg_6_prior", F.avg("balance").over(w))
    .withColumn("balance_percent_rank", F.percent_rank().over(w2))
    .withColumn("cumulative_balance", F.sum("balance").over(w3))
    .withColumn("balance_bucket", F.ntile(10).over(w4))
    .withColumn("balance_bucket_2", F.ntile(4).over(w5))
)
