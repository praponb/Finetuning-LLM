from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_subscriptions
    .filter(F.col("plan") == "basic")
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "status",
        F.count("monthly_fee").over(w).alias("running_count_monthly_fee"),
        F.min("monthly_fee").over(w).alias("running_min_monthly_fee"),
        F.sum("monthly_fee").over(w).alias("running_sum_monthly_fee"),
        F.avg("monthly_fee").over(w).alias("running_avg_monthly_fee"),
    )
)
