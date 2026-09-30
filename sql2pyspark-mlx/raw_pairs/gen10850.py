from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "status").orderBy("monthly_fee", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status", "plan").orderBy("monthly_fee", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("plan").orderBy("monthly_fee", "id")

result = (
    df_subscriptions
    .select(
        "*",
        F.avg("monthly_fee").over(w).alias("monthly_fee_avg_2"),
        F.sum("monthly_fee").over(w2).alias("running_monthly_fee"),
        F.avg("monthly_fee").over(w3).alias("monthly_fee_avg_2_prior"),
        F.sum("monthly_fee").over(w4).alias("running_monthly_fee_2"),
        F.ntile(3).over(w5).alias("monthly_fee_bucket"),
    )
)
