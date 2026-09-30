from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status", "method").orderBy("amount", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_payments
    .withColumn("cumulative_amount", F.sum("amount").over(w))
    .withColumn("amount_avg_5", F.avg("amount").over(w2))
    .withColumn("first_known_amount", F.first("amount", ignorenulls=True).over(w3))
)
