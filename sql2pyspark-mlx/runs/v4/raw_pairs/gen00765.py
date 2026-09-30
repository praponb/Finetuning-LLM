from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.partitionBy("order_id", "status").orderBy(F.col("amount").desc(), "id")
w4 = Window.partitionBy("order_id", "method").orderBy("amount", "id")

result = (
    df_payments
    .withColumn("status_avg_amount", F.avg("amount").over(w))
    .withColumn("amount_max_7d", F.max("amount").over(w2))
    .withColumn("amount_bucket", F.ntile(4).over(w3))
    .withColumn("amount_bucket_2", F.ntile(3).over(w4))
)
