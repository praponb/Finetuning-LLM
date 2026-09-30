from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("monthly_fee", "id")
w2 = Window.partitionBy("plan")

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "monthly_fee",
        F.row_number().over(w).alias("row_num"),
        F.avg("monthly_fee").over(w2).alias("plan_avg_monthly_fee"),
    )
)
