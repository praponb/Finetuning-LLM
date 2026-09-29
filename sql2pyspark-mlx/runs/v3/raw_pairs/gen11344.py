from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("end_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("plan").orderBy("start_date", "id")

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "end_date",
        "monthly_fee",
        "plan",
        "start_date",
        F.round(F.avg("monthly_fee").over(w), 2).alias("monthly_fee_moving_avg_4"),
        F.lead("monthly_fee", 1).over(w2).alias("next_monthly_fee"),
    )
)
