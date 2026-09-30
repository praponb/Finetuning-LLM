from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy("balance", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy("opened_date", "id").rowsBetween(-3, Window.currentRow)
w_hist = Window.partitionBy("customer_id").orderBy("balance", "id")

result = (
    df_accounts
    .withColumn("balance_avg_6", F.avg("balance").over(w))
    .withColumn("balance_avg_3", F.avg("balance").over(w2))
    .withColumn("row_seq", F.row_number().over(w_hist))
)
