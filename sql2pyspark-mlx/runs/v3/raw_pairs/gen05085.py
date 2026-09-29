from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("plan")

result = (
    df_subscriptions
    .filter(F.col("status") != "ACTIVE")
    .select(
        "id",
        "status",
        "monthly_fee",
        "plan",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 3).alias("monthly_fee_share"),
        F.avg("monthly_fee").over(w2).alias("plan_avg_monthly_fee"),
    )
)
