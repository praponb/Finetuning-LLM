from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("amount", "id")

with_windows = (
    df_payments
    .withColumn("cumulative_amount", F.sum("amount").over(w_ordered))
    .withColumn("amount_bucket", F.ntile(5).over(w2))
)

result = (
    with_windows
    .select("id", "order_id", "amount", "paid_at", "cumulative_amount", "amount_bucket")
    .orderBy("amount")
)
