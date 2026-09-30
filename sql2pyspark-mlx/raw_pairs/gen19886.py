from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "status").orderBy("monthly_fee", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("plan").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.month("start_date"), "customer_id")

result = (
    df_subscriptions
    .filter(F.col("status") != "ACTIVE")
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        F.avg("monthly_fee").over(w).alias("monthly_fee_avg_3"),
        F.first("monthly_fee", ignorenulls=True).over(w2).alias("first_known_monthly_fee"),
        F.last("monthly_fee", ignorenulls=True).over(w3).alias("last_known_monthly_fee"),
        F.count("monthly_fee").over(w4).alias("start_date_month_count_monthly_fee"),
    )
)
