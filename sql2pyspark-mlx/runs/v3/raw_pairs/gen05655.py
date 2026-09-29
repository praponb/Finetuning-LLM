from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("end_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("customer_id")

result = (
    df_subscriptions
    .select(
        "id",
        "end_date",
        "monthly_fee",
        "customer_id",
        F.round(F.avg("monthly_fee").over(w), 2).alias("monthly_fee_moving_avg_7"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 3).alias("monthly_fee_share"),
    )
)
