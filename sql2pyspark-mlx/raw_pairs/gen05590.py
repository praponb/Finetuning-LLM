from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("plan").orderBy("start_date", "id")

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        F.sum("monthly_fee").over(w).alias("cumulative_monthly_fee"),
        F.lag("monthly_fee", 2).over(w2).alias("prev_monthly_fee"),
    )
)
