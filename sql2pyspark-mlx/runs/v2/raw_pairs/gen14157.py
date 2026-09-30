from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("end_date")
w2 = Window.partitionBy("plan")

result = (
    df_subscriptions
    .select(
        "id",
        "end_date",
        "plan",
        "monthly_fee",
        F.rank().over(w).alias("rnk"),
        F.avg("monthly_fee").over(w2).alias("plan_avg_monthly_fee"),
    )
)
