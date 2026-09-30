from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "status").orderBy("end_date", "id")

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "status",
        "end_date",
        "monthly_fee",
        F.lag("monthly_fee", 1, 0).over(w).alias("prev_monthly_fee"),
    )
)
