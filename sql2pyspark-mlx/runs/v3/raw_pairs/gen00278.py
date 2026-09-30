from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("status")

result = (
    df_subscriptions
    .select(
        "id",
        "status",
        "end_date",
        "monthly_fee",
        F.round(F.avg("monthly_fee").over(w), 2).alias("monthly_fee_moving_avg_4"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 3).alias("monthly_fee_share"),
    )
)
