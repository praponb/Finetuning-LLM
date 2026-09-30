from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_payments
    .withColumn("amount_sum_7d", F.sum("amount").over(w))
    .withColumn("cumulative_amount", F.sum("amount").over(w2))
    .withColumn("running_amount", F.sum("amount").over(w3))
)
