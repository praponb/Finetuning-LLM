from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id")

result = (
    df_subscriptions
    .select(
        "id",
        "status",
        "customer_id",
        "monthly_fee",
        F.min("monthly_fee").over(w).alias("status_min_monthly_fee"),
    )
)
