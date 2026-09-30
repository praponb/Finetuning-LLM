from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy(F.date_trunc("month", "start_date"), "status").orderBy(F.col("monthly_fee").desc_nulls_first())
w2 = Window.partitionBy("customer_id", "plan").orderBy("monthly_fee", "id")
w3 = Window.orderBy("monthly_fee", "id").rowsBetween(-3, Window.currentRow)

with_windows = (
    df_subscriptions
    .filter(F.col("start_date") > "2023-06-20")
    .select(
        "*",
        F.cume_dist().over(w_recent).alias("monthly_fee_cume_dist"),
        F.lag("monthly_fee", 2).over(w2).alias("prev_monthly_fee"),
        F.avg("monthly_fee").over(w3).alias("monthly_fee_avg_3"),
    )
)

result = (
    with_windows
    .select("id", "plan", "start_date", "end_date", "prev_monthly_fee")
    .orderBy(F.col("start_date").desc_nulls_first())
)
