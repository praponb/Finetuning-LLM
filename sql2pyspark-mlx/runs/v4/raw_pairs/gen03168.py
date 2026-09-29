from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("end_date", "id")
w2 = Window.partitionBy("customer_id")

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "end_date",
        "monthly_fee",
        "customer_id",
        F.lag("monthly_fee", 1, 0).over(w).alias("prev_monthly_fee"),
        F.max("monthly_fee").over(w2).alias("customer_id_max_monthly_fee"),
    )
    .orderBy("id")
)
