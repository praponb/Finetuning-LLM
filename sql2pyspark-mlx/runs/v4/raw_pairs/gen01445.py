from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.orderBy("end_date", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("customer_id").orderBy("monthly_fee", "id")
w_recent = Window.partitionBy("plan", "status").orderBy("end_date", "id")

result = (
    df_subscriptions
    .withColumn("plan_max_monthly_fee", F.max("monthly_fee").over(w))
    .withColumn("monthly_fee_avg_2_prior", F.avg("monthly_fee").over(w2))
    .withColumn("running_monthly_fee", F.sum("monthly_fee").over(w3))
    .withColumn("monthly_fee_bucket", F.ntile(5).over(w_running))
    .withColumn("seq_no", F.row_number().over(w_recent))
)
