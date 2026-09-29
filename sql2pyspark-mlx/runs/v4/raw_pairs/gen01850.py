from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("opened_date", "id")
w4 = Window.partitionBy("status").orderBy(F.col("balance").asc_nulls_first())
w_hist = Window.partitionBy("customer_id", "status").orderBy(F.col("balance").desc())

result = (
    df_accounts
    .withColumn("running_balance", F.sum("balance").over(w))
    .withColumn("last_known_status", F.last("status", ignorenulls=True).over(w2))
    .withColumn("next_balance", F.lead("balance", 2).over(w_grp))
    .withColumn("balance_cume_dist", F.cume_dist().over(w4))
    .withColumn("balance_dense_rank", F.dense_rank().over(w_hist))
)
