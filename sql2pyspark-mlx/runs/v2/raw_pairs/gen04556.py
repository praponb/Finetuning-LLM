from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc(), "id")

result = (
    df_subscriptions
    .filter(F.col("status") != "EXPIRED")
    .select(
        "id",
        "plan",
        "monthly_fee",
        F.sum("monthly_fee").over(w).alias("plan_sum_monthly_fee"),
        F.ntile(10).over(w2).alias("monthly_fee_ntile"),
    )
    .orderBy(F.col("plan").desc(), F.col("plan_sum_monthly_fee").desc())
)
