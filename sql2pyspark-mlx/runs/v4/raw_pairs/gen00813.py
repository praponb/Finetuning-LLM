from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("plan")

result = (
    df_subscriptions
    .select(
        "*",
        F.last("monthly_fee", ignorenulls=True).over(w).alias("last_known_monthly_fee"),
        F.count("monthly_fee").over(w_part).alias("plan_count_monthly_fee"),
    )
)
