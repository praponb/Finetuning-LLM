from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("end_date", "id")
w2 = Window.partitionBy("customer_id").orderBy("end_date", "id")

result = (
    df_subscriptions
    .select(
        "id",
        "status",
        "end_date",
        "monthly_fee",
        "customer_id",
        F.sum("monthly_fee").over(w).alias("running_monthly_fee"),
        F.lead("monthly_fee", 1).over(w2).alias("next_monthly_fee"),
    )
)
