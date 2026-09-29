from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("method", "status").orderBy("amount", "id")
w3 = Window.partitionBy("order_id")

result = (
    df_payments
    .withColumn("cumulative_amount", F.sum("amount").over(w))
    .withColumn("prev_amount", F.lag("amount", 1).over(w2))
    .withColumn("order_id_sum_amount", F.sum("amount").over(w3))
)
