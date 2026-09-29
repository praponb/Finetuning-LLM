from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("plan")

result = (
    df_subscriptions
    .select(
        "id",
        "status",
        "monthly_fee",
        "plan",
        F.sum("monthly_fee").over(w).alias("status_sum_monthly_fee"),
        F.avg("monthly_fee").over(w2).alias("plan_avg_monthly_fee"),
    )
    .orderBy("plan_avg_monthly_fee")
)
