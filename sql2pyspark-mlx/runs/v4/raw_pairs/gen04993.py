from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(-6, -1)
w3 = Window.orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy(F.col("balance").desc())

result = (
    df_accounts
    .withColumn("first_known_status", F.first("status", ignorenulls=True).over(w_part))
    .withColumn("balance_avg_6_prior", F.avg("balance").over(w2))
    .withColumn("cumulative_balance", F.sum("balance").over(w_part))
    .withColumn("running_balance", F.sum("balance").over(w3))
    .withColumn("balance_dense_rank", F.dense_rank().over(w4))
)
