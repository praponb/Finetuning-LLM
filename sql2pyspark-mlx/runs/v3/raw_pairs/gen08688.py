from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "status")

result = (
    df_subscriptions
    .filter(F.col("status") == "CANCELLED")
    .select(
        "id",
        "plan",
        "status",
        "monthly_fee",
        F.max("monthly_fee").over(w).alias("plan_max_monthly_fee"),
    )
    .orderBy(F.col("plan").desc(), F.col("plan_max_monthly_fee").desc())
)
