from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("end_date", "id")
w2 = Window.partitionBy("plan").orderBy("start_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "end_date",
        "monthly_fee",
        "plan",
        "start_date",
        (F.col("monthly_fee") - F.lag("monthly_fee", 1).over(w)).alias("monthly_fee_change"),
        F.round(F.avg("monthly_fee").over(w2), 2).alias("monthly_fee_moving_avg_3"),
    )
)
