from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("account_type", "status").orderBy("balance", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("account_type").orderBy(F.col("balance").desc(), "id")
w4 = Window.partitionBy("customer_id", "status").orderBy(F.col("balance").desc(), "id")

result = (
    df_accounts
    .withColumn("last_known_status", F.last("status", ignorenulls=True).over(w))
    .withColumn("balance_avg_3", F.avg("balance").over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("balance_bucket", F.ntile(10).over(w4))
)
