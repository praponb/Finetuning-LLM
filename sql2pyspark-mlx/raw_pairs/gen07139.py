from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("end_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["basic", "premium", "enterprise"]))
    .select(
        "id",
        "end_date",
        "monthly_fee",
        F.round(F.avg("monthly_fee").over(w), 2).alias("monthly_fee_moving_avg_4"),
    )
    .orderBy("monthly_fee_moving_avg_4")
)
