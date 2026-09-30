from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("start_date", "id")
w2 = Window.partitionBy("plan").orderBy("end_date", "id")

result = (
    df_subscriptions
    .filter(F.col("plan") == "standard")
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        "plan",
        "end_date",
        F.sum("monthly_fee").over(w).alias("running_monthly_fee"),
        F.lead("monthly_fee", 1).over(w2).alias("next_monthly_fee"),
    )
)
