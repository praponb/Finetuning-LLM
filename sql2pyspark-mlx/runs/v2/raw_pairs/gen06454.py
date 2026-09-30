from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy("end_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "monthly_fee",
        "end_date",
        F.min("monthly_fee").over(w).alias("plan_min_monthly_fee"),
        F.round(F.avg("monthly_fee").over(w2), 2).alias("monthly_fee_moving_avg_3"),
    )
)
