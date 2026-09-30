from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "status").orderBy(F.col("amount").desc(), "id")
w_ordered = Window.partitionBy("method", "status")

scored = (
    df_payments
    .withColumn("amount_bucket", F.ntile(10).over(w))
    .withColumn("method_avg_amount", F.avg("amount").over(w_ordered))
)

result = (
    scored
    .select("id", "method", "paid_at", "status", "amount_bucket")
    .orderBy("amount_bucket")
)
