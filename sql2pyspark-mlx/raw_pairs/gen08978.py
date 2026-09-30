from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy(F.col("monthly_fee").desc(), "id")
w2 = Window.partitionBy("plan").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("end_date", "id").rowsBetween(-5, Window.currentRow)

result = (
    df_subscriptions
    .filter(F.col("start_date") > "2025-06-01")
    .select(
        "id",
        "end_date",
        F.ntile(5).over(w).alias("monthly_fee_bucket"),
        F.last("monthly_fee", ignorenulls=True).over(w2).alias("last_known_monthly_fee"),
        F.avg("monthly_fee").over(w3).alias("monthly_fee_avg_5"),
    )
)
