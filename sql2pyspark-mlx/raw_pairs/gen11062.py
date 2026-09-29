from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("plan").orderBy("monthly_fee", "id").rowsBetween(-5, -1)

result = (
    df_subscriptions
    .filter(F.col("customer_id") == 29)
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        F.first("monthly_fee", ignorenulls=True).over(w).alias("first_known_monthly_fee"),
        F.avg("monthly_fee").over(w2).alias("monthly_fee_avg_5_prior"),
    )
)
