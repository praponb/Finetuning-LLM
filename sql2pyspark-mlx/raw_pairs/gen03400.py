from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("monthly_fee", "id")
w2 = Window.orderBy("monthly_fee", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id")
w4 = Window.partitionBy("status", "plan")
w5 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "start_date",
        "monthly_fee",
        "status",
        F.row_number().over(w).alias("event_seq"),
        F.avg("monthly_fee").over(w2).alias("monthly_fee_avg_5_prior"),
        F.ntile(3).over(w3).alias("monthly_fee_bucket"),
        F.max("monthly_fee").over(w4).alias("status_max_monthly_fee"),
        F.last("monthly_fee", ignorenulls=True).over(w5).alias("last_known_monthly_fee"),
    )
)
