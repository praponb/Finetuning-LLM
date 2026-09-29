from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "method").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

result = (
    df_payments
    .withColumn("amount_bucket", F.ntile(5).over(w))
    .withColumn("amount_count_90d", F.count("amount").over(w2))
)
