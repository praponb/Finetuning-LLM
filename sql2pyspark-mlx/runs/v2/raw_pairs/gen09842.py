from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("start_date", "id")
w2 = Window.partitionBy("plan", "customer_id").orderBy("end_date", "id")

result = (
    df_subscriptions
    .select(
        "id",
        "status",
        "start_date",
        "monthly_fee",
        "plan",
        "customer_id",
        "end_date",
        (F.col("monthly_fee") - F.lag("monthly_fee", 1).over(w)).alias("monthly_fee_change"),
        F.lag("monthly_fee", 1).over(w2).alias("prev_monthly_fee"),
    )
)
