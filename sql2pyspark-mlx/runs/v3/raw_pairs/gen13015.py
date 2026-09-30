from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("end_date", "id")
w2 = Window.partitionBy("plan", "status").orderBy("end_date", "id")

result = (
    df_subscriptions
    .filter(F.col("end_date") < "2025-08-24")
    .select(
        "id",
        "customer_id",
        "end_date",
        "monthly_fee",
        "plan",
        "status",
        F.lead("monthly_fee", 1, 0).over(w).alias("next_monthly_fee"),
        F.lag("monthly_fee", 1).over(w2).alias("prev_monthly_fee"),
    )
    .orderBy("next_monthly_fee", "status")
)
