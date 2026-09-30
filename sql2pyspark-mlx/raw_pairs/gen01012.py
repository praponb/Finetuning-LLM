from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("status", "customer_id").orderBy(F.col("monthly_fee").desc(), "id")
w4 = Window.partitionBy(F.date_trunc("WEEK", "start_date")).orderBy("monthly_fee", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("plan").orderBy("start_date", "id").rowsBetween(-5, Window.currentRow)

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "monthly_fee",
        F.last("monthly_fee", ignorenulls=True).over(w).alias("last_known_monthly_fee"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4).alias("monthly_fee_share"),
        F.ntile(5).over(w3).alias("monthly_fee_bucket"),
        F.avg("monthly_fee").over(w4).alias("monthly_fee_avg_3"),
        F.avg("monthly_fee").over(w5).alias("monthly_fee_avg_5"),
    )
)
