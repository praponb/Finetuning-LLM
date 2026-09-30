from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(-5, -1)

result = (
    df_transactions
    .withColumn("amount_avg_6", F.avg("amount").over(w))
    .withColumn("amount_avg_5_prior", F.avg("amount").over(w2))
)
