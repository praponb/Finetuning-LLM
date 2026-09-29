from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("plan").orderBy("monthly_fee", "id")

result = (
    df_subscriptions
    .withColumn("last_known_monthly_fee", F.last("monthly_fee", ignorenulls=True).over(w))
    .withColumn("monthly_fee_bucket", F.ntile(3).over(w2))
)
