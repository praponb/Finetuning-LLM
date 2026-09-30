from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("plan").orderBy("end_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "end_date",
        "start_date",
        "monthly_fee",
        F.last("start_date").over(w).alias("last_start_date"),
        F.round(F.avg("monthly_fee").over(w2), 2).alias("monthly_fee_moving_avg_3"),
    )
    .orderBy(F.col("monthly_fee_moving_avg_3").desc())
)
