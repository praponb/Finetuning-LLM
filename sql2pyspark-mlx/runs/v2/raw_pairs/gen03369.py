from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_subscriptions
    .select(
        "id",
        "status",
        "start_date",
        "monthly_fee",
        F.round(F.avg("monthly_fee").over(w), 2).alias("monthly_fee_moving_avg_7"),
        F.round(F.avg("monthly_fee").over(w2), 2).alias("monthly_fee_moving_avg_3"),
    )
)
