from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "status")

result = (
    df_subscriptions
    .filter(F.col("end_date") < "2024-10-03")
    .select(
        "id",
        "plan",
        "status",
        "monthly_fee",
        F.sum("monthly_fee").over(w).alias("plan_sum_monthly_fee"),
    )
)
