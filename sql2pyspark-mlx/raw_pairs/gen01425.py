from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "monthly_fee",
        "status",
        F.last("monthly_fee", ignorenulls=True).over(w_running).alias("last_known_monthly_fee"),
        F.first("monthly_fee", ignorenulls=True).over(w_part).alias("first_known_monthly_fee"),
    )
)
