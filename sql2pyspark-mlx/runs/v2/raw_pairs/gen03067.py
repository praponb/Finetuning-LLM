from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "status").orderBy("start_date", "id")

result = (
    df_subscriptions
    .filter(F.col("status").isin(["ACTIVE", "EXPIRED", "CANCELLED"]))
    .select(
        "id",
        "plan",
        "status",
        "start_date",
        "monthly_fee",
        F.sum("monthly_fee").over(w).alias("running_monthly_fee"),
    )
)
