from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc_nulls_last())
w2 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc_nulls_last())
w3 = Window.orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status", "customer_id").orderBy("monthly_fee", "id")
w5 = Window.partitionBy("customer_id", "status")

result = (
    df_subscriptions
    .withColumn("monthly_fee_dense_rank", F.dense_rank().over(w))
    .withColumn("monthly_fee_cume_dist", F.cume_dist().over(w2))
    .withColumn("cumulative_monthly_fee", F.sum("monthly_fee").over(w3))
    .withColumn("monthly_fee_bucket", F.ntile(10).over(w4))
    .withColumn("customer_id_max_monthly_fee", F.max("monthly_fee").over(w5))
)
