from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("balance", "id")
w2 = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy(F.col("balance").asc_nulls_first())
w4 = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_accounts
    .withColumn("balance_bucket", F.ntile(4).over(w))
    .withColumn("balance_avg_6", F.avg("balance").over(w2))
    .withColumn("balance_rank", F.rank().over(w3))
    .withColumn("balance_avg_5_prior", F.avg("balance").over(w4))
    .withColumn("cumulative_balance", F.sum("balance").over(w5))
)
