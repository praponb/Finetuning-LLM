from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("start_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_subscriptions
    .filter(F.month("start_date").isin([1, 9, 11]))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "monthly_fee",
        F.min("monthly_fee").over(w).alias("last4_min_monthly_fee"),
        F.avg("monthly_fee").over(w).alias("last4_avg_monthly_fee"),
        F.count("monthly_fee").over(w).alias("last4_count_monthly_fee"),
    )
)
