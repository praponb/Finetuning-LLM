from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("plan").orderBy("monthly_fee", "id")
w3 = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc(), "id")
w4 = Window.partitionBy("plan", "customer_id")

result = (
    df_subscriptions
    .withColumn("last_known_monthly_fee", F.last("monthly_fee", ignorenulls=True).over(w))
    .withColumn("monthly_fee_bucket", F.ntile(4).over(w2))
    .withColumn("monthly_fee_bucket_2", F.ntile(5).over(w3))
    .withColumn("monthly_fee_share", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w4)), 4))
)
