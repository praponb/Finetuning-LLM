from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "start_date",
        F.count("monthly_fee").over(w).alias("running_count_monthly_fee"),
        F.min("monthly_fee").over(w).alias("running_min_monthly_fee"),
        F.avg("monthly_fee").over(w).alias("running_avg_monthly_fee"),
    )
)

result = (
    with_windows
    .select("id", "start_date", "running_count_monthly_fee", "running_avg_monthly_fee")
    .orderBy("running_avg_monthly_fee")
)
