from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")

result = (
    df_subscriptions
    .select(
        "id",
        "status",
        "monthly_fee",
        F.min("monthly_fee").over(w).alias("status_min_monthly_fee"),
    )
)
